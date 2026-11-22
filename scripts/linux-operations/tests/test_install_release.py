"""Focused release-manifest, default-off broker boot, and explicit opt-in autoreply installer tests."""
import ast
import http.server
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest

MODULE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
INSTALL_SPEC = importlib.util.spec_from_file_location('octopus_install', MODULE_DIR / 'install.py')
installer = importlib.util.module_from_spec(INSTALL_SPEC)
INSTALL_SPEC.loader.exec_module(installer)


def _node_bin():
    found = shutil.which('node')
    if not found:
        raise unittest.SkipTest('node binary required')
    return found


def _write_fake_dsh_runtime(runtime_root: Path):
    """Build a minimal isolated DSH ESM runtime layout resolvable via apps/cli/package.json."""
    cli_dir = runtime_root / 'apps' / 'cli'
    cli_dir.mkdir(parents=True, exist_ok=True)
    nm = runtime_root / 'node_modules' / '@deepseek-ai'
    pkgs = {
        'dsh-app-boot': 'export async function bootApp() { throw new Error("stub") }\n',
        'dsh-llm': 'export class LlmAdapter {}\nexport class LlmError extends Error {}\n',
        'dsh-system-prompt': 'export const SYSTEM_PROMPT_STUB = true\n',
        'dsh-launch-environment': 'export class LaunchEnvironmentSnapshot {}\n',
        'dsh-llm-deepseek-api-key': 'export const name = "llm-deepseek-api-key"\n',
    }
    for pkg_name, entry_src in pkgs.items():
        pkg_dir = nm / pkg_name
        lib_dir = pkg_dir / 'lib'
        lib_dir.mkdir(parents=True, exist_ok=True)
        (pkg_dir / 'package.json').write_text(json.dumps({
            'name': f'@deepseek-ai/{pkg_name}',
            'version': '0.0.1-test',
            'type': 'module',
            'exports': {'.': './lib/index.js'},
        }), encoding='utf-8')
        (lib_dir / 'index.js').write_text(entry_src, encoding='utf-8')
    (cli_dir / 'package.json').write_text(json.dumps({
        'name': '@deepseek-ai/dsh-cli-test',
        'version': '0.0.1-test',
        'type': 'module',
    }), encoding='utf-8')
    return cli_dir / 'package.json'


def _prepare_install_host_fixture(root: Path):
    wechat_dir = root / 'wechat'
    release_root = root / 'releases-root'
    backup_root = root / 'backups'
    state_dir = root / 'state'
    handoff_dir = root / 'handoff'
    policy_path = root / 'etc/octopus/operations.json'
    socket_path = root / 'run/broker.sock'
    ingress_db = root / 'ingress/state/ingress.sqlite'
    ingress_profile = root / 'ingress/cordis.patch.yml'
    agent_profile = root / 'agent/profile'
    unit_dir = root / 'systemd'
    source_dir = root / 'src'
    for d in [wechat_dir / 'config', wechat_dir / 'receipts', ingress_profile.parent,
              agent_profile, ingress_db.parent, socket_path.parent, source_dir]:
        d.mkdir(parents=True, exist_ok=True)

    for rel, body in [
        ('config/account.png', b'acc'),
        ('config/own-chat.png', b'own'),
        ('config/octopus-friend-label.png', b'lbl'),
        ('config/recipient.png', b'rcp'),
        ('config/recipient-sidebar.png', b'sb'),
        ('config/recipient-card.png', b'card'),
    ]:
        (wechat_dir / rel).write_bytes(body)

    (wechat_dir / 'config/config.json').write_text(json.dumps({
        'enabled': True, 'moments_enabled': True, 'direct_enabled': True,
        'account_id': 'own-id', 'visibility': 'private',
        'account_anchor': '/home/wechat/.gamma-moments/config/account.png',
        'own_chat_anchor': '/home/wechat/.gamma-moments/config/own-chat.png',
        'recipient_id': 'wx-1', 'recipient_query': '阿明',
        'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
        'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
        'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
    }), encoding='utf-8')

    binding_key = json.dumps(['feishu', 'cli_app', 'org', 'oc_chat', 'ou_owner'], separators=(',', ':'))
    (ingress_profile).write_text(
        '- insert:\n'
        '    - id: im-ingress\n'
        "      name: '@deepseek-ai/dsh-experimental-im-feishu/ingress'\n"
        '      inject: [storage]\n'
        '      config:\n'
        '        ingressId: im-ingress-production\n'
        '        stateDirectory: /var/lib/dsh-im/ingress\n'
        '        dispatchIntervalMs: 1000\n'
        f'        operatorBindings: [{json.dumps({"bindingKey": binding_key, "operatorId": "owner", "targetTenantId": "octopus"})}]\n'
        '        channel:\n'
        '          kind: feishu\n',
        encoding='utf-8',
    )
    (agent_profile / 'cordis.patch.yml').write_text(json.dumps([{'id': 'im-runtime', 'inject': ['agents']}]), encoding='utf-8')
    (agent_profile / 'package.json').write_text(json.dumps({'name': 'octopus-profile', 'dependencies': {}}), encoding='utf-8')
    (ingress_profile.parent / 'package.json').write_text(json.dumps({
        'name': 'octopus-ingress-profile',
        'dsh': {'profile': {'bundles': []}},
        'dependencies': {'@deepseek-ai/dsh-experimental-im-feishu': '0.2.1-alpha.1'},
    }), encoding='utf-8')
    (ingress_profile.parent / 'cordis.yml').write_text('[]\n', encoding='utf-8')

    for fname in installer.RELEASE_FILES:
        (source_dir / fname).write_text((MODULE_DIR / fname).read_text(encoding='utf-8'), encoding='utf-8')

    feishu_pkg = root / 'fake-feishu-pkg'
    (feishu_pkg / 'lib').mkdir(parents=True, exist_ok=True)
    (feishu_pkg / 'package.json').write_text(json.dumps({
        'name': '@deepseek-ai/dsh-experimental-im-feishu',
        'version': '0.2.1-alpha.1',
        'type': 'module',
        'exports': {'.': './lib/index.js', './ingress': './lib/ingress-plugin.js'},
    }), encoding='utf-8')
    (feishu_pkg / 'lib/index.js').write_text('export const stub = true\n', encoding='utf-8')
    (feishu_pkg / 'lib/ingress-plugin.js').write_text(
        'export const ownerNoticesCapability = {\n'
        '  supported: true,\n'
        '  schemaVersion: 1,\n'
        '  configKey: "ownerNotices",\n'
        '  handoffSchemaVersion: 1,\n'
        '  boundedNoticePump: true,\n'
        '}\n'
        'export const ingressPluginConfigSchema = { safeParse: (cfg) => ({ success: true, data: cfg }) }\n'
        'export async function apply() {}\n',
        encoding='utf-8',
    )

    return {
        'wechat_dir': wechat_dir,
        'release_root': release_root,
        'backup_root': backup_root,
        'state_dir': state_dir,
        'handoff_dir': handoff_dir,
        'policy_path': policy_path,
        'socket_path': socket_path,
        'ingress_db': ingress_db,
        'ingress_profile': ingress_profile,
        'agent_profile': agent_profile,
        'unit_dir': unit_dir,
        'source_dir': source_dir,
        'feishu_pkg': feishu_pkg,
        'binding_key': binding_key,
    }


class InstallReleaseAndAutoReplyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='dsh_ops_release_')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()

    def test_manifest_release_boots_real_broker_over_unix_socket_with_default_off_and_no_api_key_envs(self):
        node_bin = _node_bin()
        for required_file in ('wechat-autoreply.mjs', 'wechat-autoreply-dsh.mjs', 'wechat-inbox.mjs'):
            self.assertIn(required_file, installer.RELEASE_FILES)

        rel_dir = self.root / 'releases' / 'rev1'
        rel_dir.mkdir(parents=True, exist_ok=True)
        for fname in installer.RELEASE_FILES:
            shutil.copyfile(str(MODULE_DIR / fname), str(rel_dir / fname))
        current_link = self.root / 'current'
        current_link.symlink_to(rel_dir)

        state_dir = self.root / 'state'
        socket_dir = self.root / 'sock'
        wechat_dir = self.root / 'wechat'
        (wechat_dir / 'config').mkdir(parents=True, exist_ok=True)
        (wechat_dir / 'receipts').mkdir(parents=True, exist_ok=True)
        (wechat_dir / 'receipts/worker.json').write_text(json.dumps({'state': 'ready', 'heartbeat_at': int(time.time())}), encoding='utf-8')
        socket_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(socket_dir, 0o750)

        wechat_cfg = wechat_dir / 'config/config.json'
        wechat_cfg.write_text(json.dumps({'account_id': 'own-id', 'direct_enabled': True, 'recipient_id': 'wx-1', 'recipient_query': '阿明'}), encoding='utf-8')
        socket_path = socket_dir / 'broker.sock'
        policy_path = self.root / 'operations.json'
        policy_path.write_text(json.dumps({
            'socket': str(socket_path),
            'state': str(state_dir),
            'ingressDatabase': str(self.root / 'ingress.sqlite'),
            'bindingKey': json.dumps(['feishu', 'app', 'org', 'chat', 'owner'], separators=(',', ':')),
            'operatorId': 'owner',
            'tenantId': 'octopus',
            'services': {'微信': 'gamma-wechat-login.service'},
            'containers': ['gamma-api-prod'],
            'wechatRoot': str(wechat_dir),
            'wechatConfig': str(wechat_cfg),
            'wechatContacts': [
                {'id': 'friend-1', 'type': 'friend', 'alias': 'friend-1', 'displayName': '阿明',
                 'enabled': True, 'capabilities': {'directSend': True}},
            ],
            'wechatAdhocMoments': {'enabled': False},
            'wechatNotifications': {'enabled': False},
            'wechatAutoReply': {'enabled': False},
            'limits': {'logLines': 50, 'previewSeconds': 600, 'workerPollSeconds': 2, 'maxTextBytes': 1000, 'commandSeconds': 30},
        }), encoding='utf-8')
        os.chmod(policy_path, 0o600)

        clean_env = {
            k: v for k, v in os.environ.items()
            if k not in {'DEEPSEEK_API_KEY', 'OCTOPUS_WECHAT_AUTOREPLY_API_KEY'}
        }
        proc = subprocess.Popen(
            [node_bin, str(current_link / 'broker.mjs'), str(policy_path)],
            cwd=str(self.root),
            env=clean_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    out, err = proc.communicate(timeout=2)
                    self.fail(f'Broker exited early ({proc.returncode}): stdout={out!r} stderr={err!r}')
                if socket_path.exists():
                    break
                time.sleep(0.05)
            self.assertTrue(socket_path.exists(), 'Expected broker socket to bind cleanly')

            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.settimeout(5.0)
                sock.connect(str(socket_path))
                sock.sendall(b'{"action":"wechat_status","params":{}}\n')
                buf = b''
                while b'\n' not in buf:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
            finally:
                sock.close()
            reply = json.loads(buf.decode('utf-8').splitlines()[0])
            self.assertTrue(reply['ok'], reply)
            self.assertEqual(reply['result']['contacts'][0]['id'], 'friend-1')
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=2)
            if proc.stdout is not None:
                proc.stdout.close()
            if proc.stderr is not None:
                proc.stderr.close()
        self.assertEqual(proc.returncode, 0)
        self.assertFalse(socket_path.exists())

    def test_install_autoreply_default_off_opt_in_preflight_and_env_file_isolation(self):
        node_bin = _node_bin()
        f = _prepare_install_host_fixture(self.root)
        runtime_root = self.root / 'dsh-runtime'
        expected_anchor = _write_fake_dsh_runtime(runtime_root)

        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(f['socket_path']))
        srv.listen(4)
        self.addCleanup(srv.close)

        class MockCmd:
            DEVNULL = subprocess.DEVNULL
            stop_calls = []

            def call(self, cmd):
                if len(cmd) >= 3 and cmd[0] == 'systemctl' and cmd[1] == 'stop':
                    self.stop_calls.append(list(cmd))
                if len(cmd) >= 3 and cmd[0] == 'systemctl' and cmd[1] in {'is-active', 'is-enabled'}:
                    return 0
                return 0

            def check_call(self, cmd):
                if len(cmd) >= 3 and cmd[0] == 'systemctl' and cmd[1] == 'stop':
                    self.stop_calls.append(list(cmd))
                return 0

            def check_output(self, cmd, stderr=None):
                return b'0\n'

        mock_cmd = MockCmd()

        # 1. Default-off install omits autoreply activation & EnvironmentFile without needing any API key or DSH runtime.
        default_summary = installer.install(
            f['source_dir'], self.root / 'no-dsh-runtime', Path(node_bin),
            wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
            state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
            ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
            unit_dir=f['unit_dir'], handoff_dir=f['handoff_dir'], handoff_gid=os.getgid(),
            check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
        )
        self.assertEqual(default_summary['wechatAutoReply'], {'enabled': False})
        unit_text_default = (f['unit_dir'] / 'octopus-operations.service').read_text(encoding='utf-8')
        self.assertNotIn('EnvironmentFile=', unit_text_default)
        default_policy = json.loads(f['policy_path'].read_text(encoding='utf-8'))
        self.assertEqual(default_policy['wechatAutoReply'], {'enabled': False})

        # 2. Missing release file fails Phase A before stopping any service.
        broken_src = self.root / 'broken-src'
        shutil.copytree(f['source_dir'], broken_src)
        (broken_src / 'wechat-autoreply-dsh.mjs').unlink()
        mock_cmd.stop_calls.clear()
        with self.assertRaisesRegex(RuntimeError, 'Missing (source|release source) file'):
            installer.install(
                broken_src, self.root / 'no-dsh-runtime', Path(node_bin),
                wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
                state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
                ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
                unit_dir=f['unit_dir'], check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(mock_cmd.stop_calls, [])

        # 3. Embedded API secret or ambient DEEPSEEK_API_KEY without explicit allowSharedDeepSeekApiKey fails Phase A.
        env_file = self.root / 'autoreply.env'
        env_file.write_text('OCTOPUS_WECHAT_AUTOREPLY_API_KEY=sk-test-secret-never-logged\n', encoding='utf-8')
        os.chmod(env_file, 0o644)  # Too permissive at first.

        with self.assertRaisesRegex(RuntimeError, 'Do not embed secrets'):
            installer.install(
                f['source_dir'], runtime_root, Path(node_bin),
                wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
                state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
                ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
                unit_dir=f['unit_dir'], handoff_dir=f['handoff_dir'], handoff_gid=os.getgid(),
                notifications_config={'enabled': True},
                feishu_package=f['feishu_pkg'],
                autoreply_config={
                    'enabled': True,
                    'policyVersion': 1,
                    'transport': {'mode': 'deepseek-api-key', 'apiKey': 'sk-bad'},
                },
                check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(mock_cmd.stop_calls, [])

        with self.assertRaisesRegex(RuntimeError, 'group or world permissions'):
            installer.install(
                f['source_dir'], runtime_root, Path(node_bin),
                wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
                state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
                ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
                unit_dir=f['unit_dir'], handoff_dir=f['handoff_dir'], handoff_gid=os.getgid(),
                notifications_config={'enabled': True},
                feishu_package=f['feishu_pkg'],
                autoreply_config={
                    'enabled': True,
                    'policyVersion': 1,
                    'environmentFile': str(env_file),
                    'transport': {'mode': 'deepseek-api-key'},
                },
                check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(mock_cmd.stop_calls, [])
        os.chmod(env_file, 0o600)

        with self.assertRaisesRegex(RuntimeError, 'DEEPSEEK_API_KEY'):
            installer.install(
                f['source_dir'], runtime_root, Path(node_bin),
                wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
                state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
                ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
                unit_dir=f['unit_dir'], handoff_dir=f['handoff_dir'], handoff_gid=os.getgid(),
                notifications_config={'enabled': True},
                feishu_package=f['feishu_pkg'],
                autoreply_config={
                    'enabled': True,
                    'policyVersion': 1,
                    'environmentFile': str(env_file),
                    'transport': {'mode': 'deepseek-api-key', 'apiKeyEnv': 'DEEPSEEK_API_KEY'},
                },
                check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(mock_cmd.stop_calls, [])

        # 4. Broken DSH runtime lacking dsh-app-boot fails loadDshRuntimeModules preflight in Phase A before stop.
        bad_runtime = self.root / 'bad-dsh-runtime'
        (bad_runtime / 'apps/cli').mkdir(parents=True, exist_ok=True)
        (bad_runtime / 'apps/cli/package.json').write_text('{"name":"empty"}', encoding='utf-8')
        with self.assertRaisesRegex(RuntimeError, 'Unable to resolve DSH module'):
            installer.install(
                f['source_dir'], bad_runtime, Path(node_bin),
                wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
                state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
                ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
                unit_dir=f['unit_dir'], handoff_dir=f['handoff_dir'], handoff_gid=os.getgid(),
                notifications_config={'enabled': True},
                feishu_package=f['feishu_pkg'],
                autoreply_config={
                    'enabled': True,
                    'policyVersion': 1,
                    'environmentFile': str(env_file),
                    'transport': {'mode': 'deepseek-api-key'},
                },
                check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(mock_cmd.stop_calls, [])

        # 4b. Test-only transport='adapter' is rejected in Phase A (both fresh and persisted existing_policy).
        mock_cmd.stop_calls.clear()
        with self.assertRaisesRegex(RuntimeError, "test-only.*cannot boot under production startBroker"):
            installer.install(
                f['source_dir'], runtime_root, Path(node_bin),
                wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
                state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
                ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
                unit_dir=f['unit_dir'], handoff_dir=f['handoff_dir'], handoff_gid=os.getgid(),
                notifications_config={'enabled': True},
                feishu_package=f['feishu_pkg'],
                autoreply_config={'enabled': True, 'policyVersion': 1, 'transport': {'mode': 'adapter'}},
                check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(mock_cmd.stop_calls, [])

        # Even if an older policy.json on disk persisted transport.mode='adapter', reinstall must reject it in Phase A.
        corrupted_policy = json.loads(f['policy_path'].read_text(encoding='utf-8'))
        corrupted_policy['wechatNotifications'] = {'enabled': True, 'handoffDir': str(f['handoff_dir']), 'handoffGid': os.getgid(),
                                                   'receiptProducerUid': os.geteuid(), 'inboundProducerUid': os.geteuid(),
                                                   'outboxProducerUid': os.geteuid()}
        corrupted_policy['wechatAutoReply'] = {'enabled': True, 'policyVersion': 1, 'transport': {'mode': 'adapter'}}
        f['policy_path'].write_text(json.dumps(corrupted_policy), encoding='utf-8')
        with self.assertRaisesRegex(RuntimeError, "Existing enabled wechatAutoReply transport mode 'adapter' is test-only"):
            installer.install(
                f['source_dir'], runtime_root, Path(node_bin),
                wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
                state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
                ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
                unit_dir=f['unit_dir'], handoff_dir=f['handoff_dir'], handoff_gid=os.getgid(),
                feishu_package=f['feishu_pkg'],
                check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(mock_cmd.stop_calls, [])
        f['policy_path'].write_text(json.dumps(default_policy), encoding='utf-8')

        # 4c. Login-shell env vars are NOT inherited by systemd and must fail closed without a validated EnvironmentFile.
        prev_shell_key = os.environ.get('OCTOPUS_WECHAT_AUTOREPLY_API_KEY')
        os.environ['OCTOPUS_WECHAT_AUTOREPLY_API_KEY'] = 'sk-shell-only-must-not-pass'
        try:
            with self.assertRaisesRegex(RuntimeError, 'login-shell environment variables are not inherited by systemd'):
                installer.install(
                    f['source_dir'], runtime_root, Path(node_bin),
                    wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
                    state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
                    ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
                    unit_dir=f['unit_dir'], handoff_dir=f['handoff_dir'], handoff_gid=os.getgid(),
                    notifications_config={'enabled': True},
                    feishu_package=f['feishu_pkg'],
                    autoreply_config={'enabled': True, 'policyVersion': 1, 'transport': {'mode': 'deepseek-api-key'}},
                    check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
                )
        finally:
            if prev_shell_key is None:
                os.environ.pop('OCTOPUS_WECHAT_AUTOREPLY_API_KEY', None)
            else:
                os.environ['OCTOPUS_WECHAT_AUTOREPLY_API_KEY'] = prev_shell_key
        self.assertEqual(mock_cmd.stop_calls, [])

        # 4d. Canonical path, systemd-expanded path, and substituting-parent guards (while allowing safe root-owned distro /run symlinks).
        for bad_env in (
            str(self.root / '%n.env'),
            '-' + str(env_file),
            str(self.root / '*.env'),
            str(self.root / 'sub/../autoreply.env'),
        ):
            with self.assertRaisesRegex(RuntimeError, 'environmentFile'):
                installer._validate_environment_file(bad_env, check_owner=False)

        writable_parent = self.root / 'writable-parent'
        writable_parent.mkdir(mode=0o755)
        os.chmod(writable_parent, 0o777)
        in_writable = writable_parent / 'autoreply.env'
        in_writable.write_text('OCTOPUS_WECHAT_AUTOREPLY_API_KEY=sk-test\n', encoding='utf-8')
        os.chmod(in_writable, 0o600)
        with self.assertRaisesRegex(RuntimeError, 'group/world-writable'):
            installer._validate_environment_file(str(in_writable), check_owner=False)
        os.chmod(writable_parent, 0o755)

        distro_real_run = self.root / 'run-real'
        distro_real_run.mkdir(mode=0o750)
        distro_var_run = self.root / 'var-run-link'
        distro_var_run.symlink_to(distro_real_run)
        env_in_run = distro_real_run / 'autoreply.env'
        env_in_run.write_text('OCTOPUS_WECHAT_AUTOREPLY_API_KEY=sk-run-env\n', encoding='utf-8')
        os.chmod(env_in_run, 0o600)
        resolved_run_env = installer._validate_environment_file(
            str(distro_var_run / 'autoreply.env'),
            required_env_name='OCTOPUS_WECHAT_AUTOREPLY_API_KEY',
            check_owner=False,
        )
        self.assertEqual(resolved_run_env, str(env_in_run.resolve()))

        # 5. Valid explicit opt-in wires canonical EnvironmentFile=, verifies authentic staged broker startup with 0 external requests.
        recorded_http_requests = []

        class CountingHandler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                recorded_http_requests.append(self.path)
                self.send_response(500)
                self.end_headers()

            def log_message(self, format, *args):
                return

        srv_http = http.server.ThreadingHTTPServer(('127.0.0.1', 0), CountingHandler)
        srv_thread = threading.Thread(target=srv_http.serve_forever, daemon=True)
        srv_thread.start()
        self.addCleanup(srv_http.shutdown)
        self.addCleanup(srv_http.server_close)
        loopback_base_url = f'http://127.0.0.1:{srv_http.server_address[1]}/v1'

        optin_summary = installer.install(
            f['source_dir'], runtime_root, Path(node_bin),
            wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
            state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
            ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
            unit_dir=f['unit_dir'], handoff_dir=f['handoff_dir'], handoff_gid=os.getgid(),
            notifications_config={'enabled': True},
            feishu_package=f['feishu_pkg'],
            autoreply_config={
                'enabled': True,
                'policyVersion': 2,
                'environmentFile': str(distro_var_run / 'autoreply.env'),
                'projectOwnerNotices': False,
                'conversations': {'friend:friend-1': {'mode': 'draft_only'}},
                'transport': {
                    'mode': 'deepseek-api-key',
                    'model': 'deepseek-flash',
                    'baseURL': loopback_base_url,
                    'allowLoopbackHttp': True,
                },
            },
            check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
        )
        self.assertEqual(recorded_http_requests, [])
        self.assertEqual(optin_summary['wechatAutoReply'], {
            'enabled': True,
            'transportMode': 'deepseek-api-key',
            'projectOwnerNotices': False,
            'environmentFile': str(env_in_run.resolve()),
        })
        policy_raw = f['policy_path'].read_text(encoding='utf-8')
        unit_raw = (f['unit_dir'] / 'octopus-operations.service').read_text(encoding='utf-8')
        summary_raw = json.dumps(optin_summary)
        for text_blob in (policy_raw, unit_raw, summary_raw):
            self.assertNotIn('sk-run-env', text_blob)
        self.assertNotIn('environmentFile', policy_raw)
        self.assertIn(f'EnvironmentFile={env_in_run.resolve()}\n', unit_raw)
        installed_policy = json.loads(policy_raw)
        self.assertEqual(installed_policy['wechatAutoReply']['transport']['installAnchor'], str(expected_anchor.resolve()))

        # 6. Pre-installed systemd drop-in EnvironmentFile works when --autoreply-config omits environmentFile, and reinstall preserves enabled state.
        dropin_dir = f['unit_dir'] / 'octopus-operations.service.d'
        dropin_dir.mkdir(mode=0o755, exist_ok=True)
        (dropin_dir / '10-autoreply-env.conf').write_text(f'[Service]\nEnvironmentFile={env_file}\n', encoding='utf-8')
        (f['unit_dir'] / 'octopus-operations.service').write_text('[Unit]\nDescription=no-env-in-main-unit\n', encoding='utf-8')
        reinstall_summary = installer.install(
            f['source_dir'], runtime_root, Path(node_bin),
            wechat_dir=f['wechat_dir'], release_root=f['release_root'], backup_root_base=f['backup_root'],
            state_dir=f['state_dir'], policy_path=f['policy_path'], socket_path=f['socket_path'],
            ingress_db=f['ingress_db'], ingress_profile=f['ingress_profile'], agent_profile=f['agent_profile'],
            unit_dir=f['unit_dir'], handoff_dir=f['handoff_dir'], handoff_gid=os.getgid(),
            check_root=False, run_cmd=mock_cmd, chown_fn=lambda *a: None,
        )
        self.assertEqual(reinstall_summary['wechatAutoReply']['enabled'], True)
        self.assertEqual(recorded_http_requests, [])

        # 7. Real built monorepo DSH runtime preflight ALSO boots real staged broker.mjs offline with zero HTTP calls.
        real_cli_anchor = REPO_ROOT / 'apps/cli/package.json'
        if (REPO_ROOT / 'packages/boot/app/lib/index.js').is_file() and real_cli_anchor.is_file():
            real_policy = json.loads(f['policy_path'].read_text(encoding='utf-8'))
            real_autoreply, _, real_service_env_files = installer.build_autoreply_config(
                {
                    'enabled': True,
                    'policyVersion': 2,
                    'environmentFile': str(env_file),
                    'transport': {
                        'mode': 'deepseek-api-key',
                        'model': 'deepseek-flash',
                        'baseURL': loopback_base_url,
                        'allowLoopbackHttp': True,
                    },
                },
                policy_notifications=real_policy['wechatNotifications'],
                runtime_path=REPO_ROOT,
                existing_unit_path=f['unit_dir'] / 'octopus-operations.service',
                check_root=False,
                return_service_env_files=True,
            )
            real_policy['wechatAutoReply'] = real_autoreply
            real_probe = installer.probe_operations_release_surface(
                Path(node_bin),
                source_dir=f['source_dir'],
                policy=real_policy,
                runtime_path=REPO_ROOT,
                service_env_files=real_service_env_files,
                existing_config=json.loads((f['wechat_dir'] / 'config/config.json').read_text(encoding='utf-8')),
            )
            self.assertTrue(real_probe['ok'])
            self.assertEqual(real_probe['autoreplyRuntime']['installAnchor'], str(real_cli_anchor.resolve()))
            self.assertEqual(recorded_http_requests, [])

    def test_installer_preserves_python_368_grammar_and_stdlib_compatibility(self):
        install_src = (MODULE_DIR / 'install.py').read_text(encoding='utf-8')
        ast.parse(install_src, filename='install.py', feature_version=(3, 6))
        self.assertNotIn('.is_relative_to(', install_src)
        self.assertNotIn('capture_output=', install_src)
        self.assertNotIn('text=True', install_src)


if __name__ == '__main__':
    unittest.main()
