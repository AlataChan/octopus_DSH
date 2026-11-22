"""Use the deployed Gamma queue/transaction and a fake desktop; no messages are sent."""
import argparse
from contextlib import closing
from datetime import datetime
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from zoneinfo import ZoneInfo

spec = importlib.util.spec_from_file_location('octopus_worker', Path(__file__).parents[1] / 'wechat_worker.py')
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)

install_spec = importlib.util.spec_from_file_location('octopus_install', Path(__file__).parents[1] / 'install.py')
installer = importlib.util.module_from_spec(install_spec)
install_spec.loader.exec_module(installer)

from gamma_cn.moments.delivery import Queue, run_one
from gamma_cn.moments.desktop import UiUnavailable
from gamma_cn.moments.direct import DirectDesktop
from gamma_cn.moments.source import produce
from gamma_cn.moments.worker import tick

try:
    from gamma_cn.moments.desktop import AdhocMomentsDesktop, AdhocViewportPolicy
    from gamma_cn.moments.group import GroupDesktop
    from gamma_cn.moments.delivery import AdhocMomentsQueue, CHANNEL_TARGETS, reconcile
    HAS_PHASE2_GAMMA = 'group' in CHANNEL_TARGETS and 'adhoc_moments' in CHANNEL_TARGETS
except ImportError:
    AdhocMomentsDesktop = object
    AdhocViewportPolicy = None
    GroupDesktop = object
    AdhocMomentsQueue = None
    reconcile = None
    HAS_PHASE2_GAMMA = False


class Desktop:
    submissions = []
    fail_after_submit = False

    def __init__(self, config):
        self.config = config

    def identity(self):
        return worker.sha(self.config['account_id'])

    def _open_recipient(self, recipient):
        if recipient != self.config['recipient_id']:
            raise ValueError('recipient_mismatch')

    def prepare(self, text, recipient, *, trade_date):
        self.text = text
        return {'text': text, 'recipient_id': recipient, 'account_sha256': self.identity()}

    def preflight(self):
        return True

    def submit(self):
        self.submissions.append(self.text)
        if self.fail_after_submit:
            raise RuntimeError('uncertain_submission')

    def verify(self, text, recipient):
        return text == self.text and recipient == self.config['recipient_id']


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / 'adhoc' / 'outbox').mkdir(parents=True)
        (self.root / 'adhoc' / 'receipts').mkdir()
        (self.root / 'receipts').mkdir()
        self.config = {'enabled': True, 'direct_enabled': True, 'account_id': 'own-id', 'recipient_id': 'bound-id'}
        Desktop.submissions = []
        Desktop.fail_after_submit = False

    def job(self, identifier='a' * 24, text='first'):
        now = int(time.time() * 1000)
        job = {'schema_version': 1, 'kind': 'send', 'job_id': identifier, 'account_sha256': worker.sha('own-id'),
               'recipient_id': 'bound-id', 'recipient_alias': '默认联系人', 'text': text,
               'text_sha256': worker.sha(text), 'created_at': now, 'expires_at': now + 600000,
               'confirmation_request_id': 'authenticated-request'}
        path = self.root / 'adhoc' / 'outbox' / (identifier + '.json')
        path.write_text(json.dumps(job))
        path.chmod(0o640)
        return path

    def run_job(self, path):
        return worker.process_job(path, self.root, self.config, Queue, run_one, Desktop, trusted_uid=os.getuid())

    def test_multiple_same_day_messages_and_duplicate_ticks(self):
        first = self.job()
        self.assertEqual(self.run_job(first)['state'], 'sent_verified')
        self.assertEqual(self.run_job(first)['state'], 'sent_verified')
        self.assertEqual(self.run_job(self.job('b' * 24, 'second'))['state'], 'sent_verified')
        self.assertEqual(Desktop.submissions, ['first', 'second'])

    def test_crash_after_click_is_uncertain_and_never_replayed(self):
        path = self.job()
        Desktop.fail_after_submit = True
        self.assertEqual(self.run_job(path)['state'], 'needs_review')
        Desktop.fail_after_submit = False
        self.assertEqual(self.run_job(path)['state'], 'needs_review')
        self.assertEqual(Desktop.submissions, ['first'])

    def test_worker_restart_during_preparation_does_not_type_again(self):
        path = self.job()
        worker.save(self.root / 'adhoc' / 'receipts' / path.name, {'state': 'prepared'})
        self.assertEqual(self.run_job(path)['state'], 'needs_review')
        self.assertEqual(Desktop.submissions, [])

    def test_shared_account_lock_blocks_another_ui_owner(self):
        path = self.job()
        queue = Queue(self.root / 'outbox', self.root / 'receipts')
        with queue.lock(worker.sha('own-id')):
            self.assertEqual(self.run_job(path)['state'], 'busy')
        self.assertEqual(Desktop.submissions, [])
        self.assertEqual(self.run_job(path)['state'], 'sent_verified')

    def test_changed_recipient_or_content_is_refused(self):
        path = self.job()
        value = json.loads(path.read_text())
        value['text'] = 'changed'
        path.write_text(json.dumps(value))
        self.assertEqual(self.run_job(path)['state'], 'refused')
        self.assertEqual(Desktop.submissions, [])

    def test_expiry_and_untrusted_writable_files_are_refused(self):
        path = self.job()
        value = json.loads(path.read_text())
        value['expires_at'] = value['created_at'] - 1
        path.write_text(json.dumps(value))
        self.assertEqual(self.run_job(path)['state'], 'refused')
        other = self.job('c' * 24)
        other.chmod(0o666)
        self.assertEqual(self.run_job(other)['state'], 'refused')

    def test_identity_probe_never_submits(self):
        path = self.job()
        value = json.loads(path.read_text())
        value['kind'] = 'probe'
        path.write_text(json.dumps(value))
        self.assertEqual(self.run_job(path)['state'], 'identity_verified')
        self.assertEqual(Desktop.submissions, [])

    def test_label_fallback_requires_unique_visual_evidence(self):
        rows = [{'text': '其他文字', 'box': (0, 0, 10, 10)}]
        restored = worker.label_rows(rows, object(), object(), lambda image, template: (20, 30, 40, 15))
        self.assertEqual(restored[-1], {'text': '朋友资料', 'box': (20, 30, 40, 15)})
        def missing(image, template):
            raise ValueError('visual_anchor_not_unique')
        self.assertEqual(worker.label_rows(rows, object(), object(), missing), rows)
        self.assertEqual(worker.label_rows(restored, object(), object(), missing), restored)

    def test_separate_id_value_is_targeted_only_when_unique_and_aligned(self):
        label = {'text': '微信号', 'box': (100, 108, 37, 20)}
        value = {'text': 'bound-id', 'box': (154, 112, 60, 14)}
        self.assertEqual(worker.recipient_id_rows([label, value], 'bound-id')[0]['box'], value['box'])
        self.assertEqual(worker.recipient_id_rows([label, value], 'other-id'), [label, value])
        duplicate = {**value, 'box': (155, 112, 60, 14)}
        self.assertEqual(worker.recipient_id_rows([label, value, duplicate], 'bound-id'), [label, value, duplicate])
        lower = {**value, 'box': (154, 160, 60, 14)}
        self.assertEqual(worker.recipient_id_rows([label, lower], 'bound-id'), [label, lower])


class MultiContactDesktop(Desktop):
    def _open_recipient(self, recipient):
        if recipient != self.config['recipient_id']:
            raise ValueError('recipient_mismatch')

    def verify(self, text, recipient):
        return text == self.text and recipient == self.config['recipient_id']


class MultiContactAndMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for sub in ['adhoc/outbox', 'adhoc/receipts', 'outbox', 'receipts', 'config/contacts/friend-2', 'config/contacts/friend-3']:
            (self.root / sub).mkdir(parents=True, exist_ok=True)
        for rel, body in [
            ('config/account.png', b'acc'),
            ('config/own-chat.png', b'own'),
            ('config/octopus-friend-label.png', b'label'),
            ('config/recipient.png', b'f1-s'),
            ('config/recipient-sidebar.png', b'f1-sb'),
            ('config/recipient-card.png', b'f1-c'),
            ('config/contacts/friend-2/recipient.png', b'f2-s'),
            ('config/contacts/friend-2/recipient-sidebar.png', b'f2-sb'),
            ('config/contacts/friend-2/recipient-card.png', b'f2-c'),
            ('config/contacts/friend-3/recipient.png', b'f3-s'),
            ('config/contacts/friend-3/recipient-sidebar.png', b'f3-sb'),
            ('config/contacts/friend-3/recipient-card.png', b'f3-c'),
        ]:
            (self.root / rel).write_bytes(body)
        self.config_path = self.root / 'config' / 'config.json'
        self.contacts_path = self.root / 'config' / 'octopus-contacts.json'
        self.config = {
            'enabled': True, 'moments_enabled': True, 'direct_enabled': True,
            'account_id': 'own-id', 'visibility': 'private',
            'evidence_dir': str(self.root / 'receipts' / 'evidence'),
            'account_anchor': '/home/wechat/.gamma-moments/config/account.png',
            'own_chat_anchor': '/home/wechat/.gamma-moments/config/own-chat.png',
            'recipient_id': 'wx-1', 'recipient_query': '阿明',
            'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
            'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
            'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
        }
        self.sidecar = {
            'schema_version': 1,
            'account_sha256': worker.sha('own-id'),
            'contacts': [
                {
                    'id': 'friend-1', 'type': 'friend', 'alias': '阿明-北京', 'display_name': '阿明',
                    'enabled': True, 'direct_enabled': True, 'legacy': True,
                    'recipient_id': 'wx-1', 'recipient_query': '阿明',
                    'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
                    'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
                    'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
                },
                {
                    'id': 'friend-2', 'type': 'friend', 'alias': '好友二', 'display_name': '阿强',
                    'enabled': True, 'direct_enabled': True,
                    'recipient_id': 'wx-2', 'recipient_query': '阿强',
                    'recipient_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient.png',
                    'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-sidebar.png',
                    'recipient_card_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-card.png',
                },
                {
                    'id': 'friend-3', 'type': 'friend', 'alias': '阿明-上海', 'display_name': '阿明',
                    'enabled': True, 'direct_enabled': True,
                    'recipient_id': 'wx-3', 'recipient_query': '阿明',
                    'recipient_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient.png',
                    'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient-sidebar.png',
                    'recipient_card_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient-card.png',
                },
            ],
        }
        self.config_path.write_text(json.dumps(self.config), encoding='utf-8')
        self.contacts_path.write_text(json.dumps(self.sidecar), encoding='utf-8')
        self.contacts_path.chmod(0o640)
        Desktop.submissions = []
        Desktop.fail_after_submit = False

    def v2_job(self, identifier, contact_index, text='hello', *, recipient_id=None, contact_type='friend', binding_override=None):
        contact = self.sidecar['contacts'][contact_index]
        now = int(time.time() * 1000)
        binding = binding_override or worker.compute_binding_sha256(self.root, self.config, contact)
        job = {
            'schema_version': 2, 'kind': 'send', 'job_id': identifier,
            'account_sha256': worker.sha('own-id'),
            'contact_id': contact['id'], 'contact_type': contact_type,
            'recipient_id': recipient_id or contact['recipient_id'],
            'recipient_alias': contact['alias'],
            'binding_sha256': binding,
            'text': text, 'text_sha256': worker.sha(text),
            'created_at': now, 'expires_at': now + 600000,
            'confirmation_request_id': 'auth-req',
        }
        path = self.root / 'adhoc' / 'outbox' / (identifier + '.json')
        path.write_text(json.dumps(job), encoding='utf-8')
        path.chmod(0o640)
        return path

    def test_three_contacts_dispatch_separately_and_reject_substitution_or_tamper(self):
        p1 = self.v2_job('1' * 24, 0, 'msg1')
        p2 = self.v2_job('2' * 24, 1, 'msg2')
        p3 = self.v2_job('3' * 24, 2, 'msg3')
        for path in (p1, p2, p3):
            res = worker.process_job(path, self.root, self.config, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid())
            self.assertEqual(res['state'], 'sent_verified')
        self.assertEqual(Desktop.submissions, ['msg1', 'msg2', 'msg3'])

        bad_sub = self.v2_job('4' * 24, 1, 'sub', recipient_id='wx-3', binding_override='0' * 64)
        self.assertEqual(worker.process_job(bad_sub, self.root, self.config, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid())['state'], 'refused')

        bad_group = self.v2_job('5' * 24, 1, 'grp', contact_type='group')
        self.assertEqual(worker.process_job(bad_group, self.root, self.config, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid())['state'], 'refused')

        p_anchor = self.v2_job('6' * 24, 1, 'anchor-tamper')
        (self.root / 'config/contacts/friend-2/recipient-card.png').write_bytes(b'replaced-anchor')
        self.assertEqual(worker.process_job(p_anchor, self.root, self.config, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid())['state'], 'refused')
        self.assertEqual(Desktop.submissions, ['msg1', 'msg2', 'msg3'])

    def test_unbound_third_contact_does_not_block_bound_contacts(self):
        p1 = self.v2_job('d' * 24, 0, 'bound-f1')
        (self.root / 'config/contacts/friend-3/recipient-card.png').unlink()
        res1 = worker.process_job(p1, self.root, self.config, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid())
        self.assertEqual(res1['state'], 'sent_verified')
        bad_f3 = self.v2_job('f' * 24, 2, 'unbound-f3', binding_override='0' * 64)
        res3 = worker.process_job(bad_f3, self.root, self.config, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid())
        self.assertEqual(res3['state'], 'refused')

    def test_presubmit_recheck_blocks_rebound_or_disabled_contact_before_submit(self):
        path = self.v2_job('7' * 24, 1, 'recheck')

        class MutatingDesktop(MultiContactDesktop):
            def prepare(inner_self, text, recipient, *, trade_date):
                result = super().prepare(text, recipient, trade_date=trade_date)
                self.sidecar['contacts'][1]['recipient_id'] = 'wx-hijacked'
                self.contacts_path.write_text(json.dumps(self.sidecar), encoding='utf-8')
                return result

        res = worker.process_job(
            path, self.root, self.config, Queue, run_one, MutatingDesktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
        )
        self.assertEqual(res['state'], 'failed_before_submit')
        self.assertEqual(Desktop.submissions, [])

    def test_gamma_config_rewrite_preserves_sidecar_and_fails_closed_on_account_or_recipient_change(self):
        # Gamma's deploy_wechat_moments.py overwrites config/config.json from scratch without contacts.
        gamma_rewritten = {
            'enabled': True, 'moments_enabled': True, 'direct_enabled': True,
            'account_id': 'own-id',
            'recipient_id': 'wx-1', 'recipient_query': '阿明',
            'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
            'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
            'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
            'own_chat_anchor': '/home/wechat/.gamma-moments/config/own-chat.png',
            'moments_not_before': '',
            'account_anchor': '/home/wechat/.gamma-moments/config/account.png',
            'visibility': 'friends',
        }
        self.config_path.write_text(json.dumps(gamma_rewritten), encoding='utf-8')
        p2 = self.v2_job('1' * 24, 1, 'after-gamma-rewrite')
        res2 = worker.process_job(p2, self.root, gamma_rewritten, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid())
        self.assertEqual(res2['state'], 'sent_verified')

        # If Gamma rewrites recipient_id, legacy contact v1 & v2 fail closed; if account_id changes, all contacts fail closed.
        changed_recipient = dict(gamma_rewritten, recipient_id='wx-changed')
        p1_v2 = self.v2_job('2' * 24, 0, 'legacy-v2')
        self.assertEqual(worker.process_job(p1_v2, self.root, changed_recipient, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid())['state'], 'refused')

        changed_account = dict(gamma_rewritten, account_id='other-account')
        p2_other = self.v2_job('3' * 24, 1, 'wrong-account')
        self.assertEqual(worker.process_job(p2_other, self.root, changed_account, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid())['state'], 'refused')

    def test_rejected_installer_config_preserves_active_service_and_prior_release(self):
        import shutil
        deploy_root = self.root / 'reject-sim'
        wechat_dir = deploy_root / 'wechat'
        release_root = deploy_root / 'releases-root'
        backup_root = deploy_root / 'backups'
        state_dir = deploy_root / 'state'
        policy_path = deploy_root / 'etc/octopus/operations.json'
        socket_path = deploy_root / 'run/octopus-ops/broker.sock'
        ingress_db = deploy_root / 'ingress/state/ingress.sqlite'
        ingress_profile = deploy_root / 'ingress/cordis.patch.yml'
        agent_profile = deploy_root / 'agent/profile'
        unit_dir = deploy_root / 'systemd'
        prev_release = release_root / 'releases/previous'
        for d in [wechat_dir / 'config', wechat_dir / 'receipts', ingress_profile.parent, agent_profile,
                  ingress_db.parent, socket_path.parent, prev_release, policy_path.parent]:
            d.mkdir(parents=True, exist_ok=True)
        for rel, body in [
            ('config/account.png', b'acc'), ('config/own-chat.png', b'own'),
            ('config/octopus-friend-label.png', b'lbl'),
            ('config/recipient.png', b'f1s'), ('config/recipient-sidebar.png', b'f1sb'), ('config/recipient-card.png', b'f1c'),
        ]:
            (wechat_dir / rel).write_bytes(body)
        original_gamma_cfg = json.dumps({
            'enabled': True, 'moments_enabled': True, 'direct_enabled': True, 'account_id': 'own-id', 'visibility': 'private',
            'account_anchor': '/home/wechat/.gamma-moments/config/account.png',
            'own_chat_anchor': '/home/wechat/.gamma-moments/config/own-chat.png',
            'recipient_id': 'wx-1', 'recipient_query': '阿明',
            'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
            'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
            'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
        })
        (wechat_dir / 'config/config.json').write_text(original_gamma_cfg)
        prior_sidecar_text = json.dumps({'schema_version': 1, 'account_sha256': worker.sha('own-id'), 'contacts': [self.sidecar['contacts'][0]]})
        (wechat_dir / 'config/octopus-contacts.json').write_text(prior_sidecar_text)
        prior_policy_text = json.dumps({'prior': True, 'limits': {'logLines': 50, 'previewSeconds': 600, 'workerPollSeconds': 2, 'maxTextBytes': 1000, 'commandSeconds': 30}})
        policy_path.write_text(prior_policy_text)
        current_link = release_root / 'current'
        current_link.symlink_to(prev_release)
        binding_key = json.dumps(['feishu', 'app', 'org', 'chat', 'owner'], separators=(',', ':'))
        ingress_profile.write_text(f"operatorBindings: {json.dumps([{'bindingKey': binding_key, 'operatorId': 'owner', 'targetTenantId': 'octopus'}])}\n")
        (agent_profile / 'cordis.patch.yml').write_text(json.dumps([{'id': 'im-runtime', 'inject': ['agents']}]))
        (agent_profile / 'package.json').write_text(json.dumps({'name': 'octopus-profile', 'dependencies': {}}))
        invalid_contacts = deploy_root / 'invalid-contacts.json'
        invalid_contacts.write_text(json.dumps([
            {'id': 'bad-group', 'type': 'group', 'alias': '群聊', 'display_name': '群聊', 'enabled': True, 'direct_enabled': True},
        ]))

        class ActiveServiceCmd:
            calls = []
            def call(self, args):
                self.calls.append(args)
                return 0
            def check_call(self, args):
                self.calls.append(args)
            def check_output(self, args, stderr=None):
                self.calls.append(args)
                return b'0\n'

        fake_cmd = ActiveServiceCmd()
        with self.assertRaises(RuntimeError):
            installer.install(
                Path(__file__).parents[1], Path('/usr/bin/podman'), Path(shutil.which('node')),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, contacts_config=invalid_contacts, check_root=False,
                run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertFalse(any(c[:2] == ['systemctl', 'stop'] for c in fake_cmd.calls))
        self.assertEqual(Path(os.readlink(current_link)), prev_release)
        self.assertEqual((wechat_dir / 'config/octopus-contacts.json').read_text(), prior_sidecar_text)
        self.assertEqual(policy_path.read_text(), prior_policy_text)

    def test_old_v1_jobs_only_target_legacy_original_contact(self):
        now = int(time.time() * 1000)
        for jid, rid, expected in [('a' * 24, 'wx-1', 'sent_verified'), ('b' * 24, 'wx-2', 'refused'), ('c' * 24, 'wx-3', 'refused')]:
            job = {
                'schema_version': 1, 'kind': 'send', 'job_id': jid,
                'account_sha256': worker.sha('own-id'), 'recipient_id': rid,
                'recipient_alias': '默认联系人', 'text': 'old', 'text_sha256': worker.sha('old'),
                'created_at': now, 'expires_at': now + 600000, 'confirmation_request_id': 'req',
            }
            path = self.root / 'adhoc' / 'outbox' / (jid + '.json')
            path.write_text(json.dumps(job), encoding='utf-8')
            path.chmod(0o640)
            res = worker.process_job(path, self.root, self.config, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid())
            self.assertEqual(res['state'], expected)

    def test_install_migration_and_three_contact_setup_preserve_gamma_daily(self):
        legacy_cfg = {
            'enabled': True, 'moments_enabled': True, 'direct_enabled': True,
            'account_id': 'own-id', 'visibility': 'private',
            'account_anchor': '/home/wechat/.gamma-moments/config/account.png',
            'own_chat_anchor': '/home/wechat/.gamma-moments/config/own-chat.png',
            'recipient_id': 'wx-1', 'recipient_query': '阿明',
            'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
            'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
            'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
        }
        migrated_sidecar, policy_contacts = installer.migrate_legacy_config(legacy_cfg, default_alias='默认联系人')
        self.assertEqual(len(policy_contacts), 1)
        self.assertEqual(policy_contacts[0]['legacy'], True)
        self.assertEqual(migrated_sidecar['account_sha256'], worker.sha('own-id'))

        contacts_spec = [
            {
                'id': 'friend-1', 'type': 'friend', 'alias': '阿明-北京', 'display_name': '阿明',
                'enabled': True, 'direct_enabled': True, 'legacy': True,
                'recipient_id': 'wx-1', 'recipient_query': '阿明',
                'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
                'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
                'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
            },
            {
                'id': 'friend-2', 'type': 'friend', 'alias': '好友二', 'display_name': '阿强',
                'enabled': True, 'direct_enabled': True,
                'recipient_id': 'wx-2', 'recipient_query': '阿强',
                'recipient_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient.png',
                'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-sidebar.png',
                'recipient_card_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-card.png',
            },
            {
                'id': 'friend-3', 'type': 'friend', 'alias': '阿明-上海', 'display_name': '阿明',
                'enabled': True, 'direct_enabled': True,
                'recipient_id': 'wx-3', 'recipient_query': '阿明',
                'recipient_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient.png',
                'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient-sidebar.png',
                'recipient_card_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient-card.png',
            },
        ]
        three_sidecar, three_policy = installer.build_contacts_registry(
            contacts_spec, legacy_cfg, wechat_dir=self.root, check_owner=False,
        )
        self.assertEqual(len(three_policy), 3)
        self.assertFalse(any('recipient_id' in item for item in three_policy))
        self.assertEqual(three_sidecar['schema_version'], 1)
        self.contacts_path.write_text(json.dumps(three_sidecar), encoding='utf-8')
        self.contacts_path.chmod(0o640)

        # Existing Gamma daily producer/worker compatibility check (Gamma config.json stays strictly Gamma-shaped)
        self.config_path.write_text(json.dumps(legacy_cfg), encoding='utf-8')
        from datetime import datetime
        from zoneinfo import ZoneInfo
        now = datetime(2026, 11, 6, 18, 0, tzinfo=ZoneInfo('Asia/Shanghai'))
        out = produce(self.root / 'missing-journal.jsonl', self.root / 'runs', Queue(self.root / 'outbox', self.root / 'receipts'),
                      now=now, account='own-id', visibility='private', recipient='wx-1')
        self.assertEqual(out['state'], 'awaiting_report')
        class LoggedIn(MultiContactDesktop):
            def logged_in(self):
                return True
        tick_status = tick(self.root, self.config_path, desktop=LoggedIn(legacy_cfg), clock=lambda: now)
        self.assertEqual(tick_status['state'], 'ready')

    def test_real_gamma_direct_desktop_signature_isolates_contacts_and_rejects_wrong_card(self):
        from PIL import Image
        import numpy as np
        # Create valid non-flat PNG templates (find_template requires template.std() >= 5)
        assets = self.root / 'assets'
        assets.mkdir(exist_ok=True)
        def write_pattern(path, seed):
            rng = np.random.default_rng(seed)
            arr = rng.integers(0, 255, (16, 16, 3), dtype=np.uint8)
            Image.fromarray(arr).save(path)
        for name, seed in [
            ('account.png', 1), ('chat-search.png', 2), ('chat-contacts.png', 3),
            ('chat-network.png', 4), ('chat-details.png', 5), ('chat-message.png', 6),
            ('copy.png', 7), ('chat-tools.png', 8), ('chat-send.png', 9), ('chat-copy.png', 10),
        ]:
            write_pattern(assets / name, seed)
        write_pattern(self.root / 'config/account.png', 1)
        write_pattern(self.root / 'config/own-chat.png', 11)
        write_pattern(self.root / 'config/contacts/friend-2/recipient.png', 21)
        write_pattern(self.root / 'config/contacts/friend-2/recipient-sidebar.png', 22)
        write_pattern(self.root / 'config/contacts/friend-2/recipient-card.png', 23)
        write_pattern(self.root / 'config/contacts/friend-3/recipient.png', 31)
        write_pattern(self.root / 'config/contacts/friend-3/recipient-sidebar.png', 32)
        write_pattern(self.root / 'config/contacts/friend-3/recipient-card.png', 33)

        class SyntheticDirectDesktop(DirectDesktop):
            def __init__(inner_self, cfg):
                inner_self._clip = ''
                inner_self._stage = 'init'
                inner_self._sent = False
                super().__init__(cfg, assets=assets, runner=inner_self._run_cmd)
            def identity(inner_self):
                return worker.sha(inner_self.config['account_id'])
            def window(inner_self, title):
                return {'id': 'win-1', 'title': '微信', 'box': (0, 0, 1280, 900)}
            def clipboard_set(inner_self, text):
                inner_self._clip = text
            def clipboard_get(inner_self):
                return inner_self._clip
            def visual_box(inner_self, window, name):
                boxes = {'chat-search': (80, 40, 20, 20), 'chat-contacts': (80, 80, 40, 15),
                         'chat-network': (80, 300, 40, 15), 'chat-message': (200, 500, 40, 15),
                         'chat-tools': (420, 760, 20, 20), 'chat-send': (1100, 860, 40, 20)}
                if name not in boxes:
                    raise UiUnavailable('visual_anchor_missing')
                return boxes[name]
            def _private_anchor(inner_self, window, name, *, region=None):
                if name == 'own_chat_anchor':
                    return (1220, 150, 30, 30)
                return (100, 120, 16, 16)
            def icon_click(inner_self, window, name, *, hold=False):
                if name == 'copy':
                    inner_self._clip = inner_self.config['recipient_id']
                elif name == 'chat-copy':
                    inner_self._clip = 'real-msg'
            def capture(inner_self, window):
                img = np.full((900, 1280, 3), 248, dtype=np.uint8)
                if window['box'][2] == 292:
                    return img, [{'text': '微信号:' + inner_self.config['recipient_id'], 'box': (20, 100, 120, 15)},
                                 {'text': '朋友资料', 'box': (20, 140, 60, 15)},
                                 {'text': '发消息', 'box': (20, 500, 40, 15)}]
                if inner_self._sent:
                    img[150:190, 900:1200] = (149, 236, 105)
                return img, [{'text': '清空聊天记录', 'box': (1100, 200, 80, 15)}]
            def _read_composer(inner_self):
                return '' if not hasattr(inner_self, '_draft') else inner_self._draft
            def _run_cmd(inner_self, *args, **kwargs):
                if args[:2] == ('xdotool', 'key') and args[2] == 'ctrl+v':
                    inner_self._draft = inner_self._clip
                return ''
            def submit(inner_self):
                inner_self._sent = True
                inner_self._draft = ''
                super().submit()

        p2 = self.v2_job('8' * 24, 1, 'real-msg')
        res = worker.process_job(p2, self.root, self.config, Queue, run_one, SyntheticDirectDesktop, trusted_uid=os.getuid())
        self.assertEqual(res['state'], 'sent_verified')
        self.assertEqual(res['proof']['recipient_sha256'], worker.sha('wx-2'))

        # Verify wrong card ID and group sidebar both reject via real DirectDesktop._open_recipient
        class WrongCardDesktop(SyntheticDirectDesktop):
            def icon_click(inner_self, window, name, *, hold=False):
                if name == 'copy':
                    inner_self._clip = 'wx-3'
                else:
                    super().icon_click(window, name, hold=hold)

        p2_wrong = self.v2_job('9' * 24, 1, 'real-msg')
        res_wrong = worker.process_job(p2_wrong, self.root, self.config, Queue, run_one, WrongCardDesktop, trusted_uid=os.getuid())
        self.assertEqual(res_wrong['state'], 'failed_before_submit')
        self.assertEqual(res_wrong['reason'], 'recipient_identity_mismatch')

        class GroupSidebarDesktop(SyntheticDirectDesktop):
            def capture(inner_self, window):
                img, rows = super().capture(window)
                if window['box'][2] != 292:
                    return img, [{'text': '群公告', 'box': (1100, 200, 80, 15)}]
                return img, rows

        p2_grp = self.v2_job('e' * 24, 1, 'real-msg')
        res_grp = worker.process_job(p2_grp, self.root, self.config, Queue, run_one, GroupSidebarDesktop, trusted_uid=os.getuid())
        self.assertEqual(res_grp['state'], 'failed_before_submit')
        self.assertEqual(res_grp['reason'], 'group_conversation_rejected')

    def test_end_to_end_installer_three_contacts_and_node_broker_to_python_worker_handoff(self):
        import shutil
        import socket
        import subprocess
        from types import SimpleNamespace

        deploy_root = self.root / 'deploy-sim'
        wechat_dir = deploy_root / 'wechat'
        release_root = deploy_root / 'releases-root'
        backup_root = deploy_root / 'backups'
        state_dir = deploy_root / 'state'
        policy_path = deploy_root / 'etc/octopus/operations.json'
        socket_path = deploy_root / 'run/octopus-ops/broker.sock'
        ingress_db = deploy_root / 'ingress/state/ingress.sqlite'
        ingress_profile = deploy_root / 'ingress/cordis.patch.yml'
        agent_profile = deploy_root / 'agent/profile'
        unit_dir = deploy_root / 'systemd'
        for d in [wechat_dir / 'config/contacts/friend-2', wechat_dir / 'config/contacts/friend-3',
                  wechat_dir / 'receipts', ingress_profile.parent, agent_profile, ingress_db.parent, socket_path.parent]:
            d.mkdir(parents=True, exist_ok=True)
        for rel, body in [
            ('config/account.png', b'acc'), ('config/own-chat.png', b'own'),
            ('config/octopus-friend-label.png', b'lbl'),
            ('config/recipient.png', b'f1s'), ('config/recipient-sidebar.png', b'f1sb'), ('config/recipient-card.png', b'f1c'),
            ('config/contacts/friend-2/recipient.png', b'f2s'), ('config/contacts/friend-2/recipient-sidebar.png', b'f2sb'), ('config/contacts/friend-2/recipient-card.png', b'f2c'),
            ('config/contacts/friend-3/recipient.png', b'f3s'), ('config/contacts/friend-3/recipient-sidebar.png', b'f3sb'), ('config/contacts/friend-3/recipient-card.png', b'f3c'),
        ]:
            (wechat_dir / rel).write_bytes(body)
        (wechat_dir / 'receipts/worker.json').write_text(json.dumps({'checked_at': '2026-11-21T00:00:00+08:00', 'state': 'ready'}))
        original_gamma_cfg = json.dumps({
            'enabled': True, 'moments_enabled': True, 'direct_enabled': True, 'account_id': 'own-id', 'visibility': 'private',
            'account_anchor': '/home/wechat/.gamma-moments/config/account.png',
            'own_chat_anchor': '/home/wechat/.gamma-moments/config/own-chat.png',
            'recipient_id': 'wx-1', 'recipient_query': '阿明',
            'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
            'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
            'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
        })
        (wechat_dir / 'config/config.json').write_text(original_gamma_cfg)
        binding_key = json.dumps(['feishu', 'app', 'org', 'chat', 'owner'], separators=(',', ':'))
        ingress_profile.write_text(f"operatorBindings: {json.dumps([{'bindingKey': binding_key, 'operatorId': 'owner', 'targetTenantId': 'octopus'}])}\n")
        (agent_profile / 'cordis.patch.yml').write_text(json.dumps([{'id': 'im-runtime', 'inject': ['agents']}]))
        (agent_profile / 'package.json').write_text(json.dumps({'name': 'octopus-profile', 'dependencies': {}}))
        contacts_file = deploy_root / 'contacts.json'
        contacts_file.write_text(json.dumps([
            {'id': 'friend-1', 'type': 'friend', 'alias': '阿明-北京', 'display_name': '阿明', 'enabled': True, 'direct_enabled': True, 'legacy': True,
             'recipient_id': 'wx-1', 'recipient_query': '阿明',
             'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
             'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
             'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png'},
            {'id': 'friend-2', 'type': 'friend', 'alias': '好友二', 'display_name': '阿强', 'enabled': True, 'direct_enabled': True,
             'recipient_id': 'wx-2', 'recipient_query': '阿强',
             'recipient_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient.png',
             'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-sidebar.png',
             'recipient_card_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-card.png'},
            {'id': 'friend-3', 'type': 'friend', 'alias': '阿明-上海', 'display_name': '阿明', 'enabled': True, 'direct_enabled': True,
             'recipient_id': 'wx-3', 'recipient_query': '阿明',
             'recipient_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient.png',
             'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient-sidebar.png',
             'recipient_card_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-3/recipient-card.png'},
        ]))
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(socket_path))
        srv.listen(8)
        self.addCleanup(srv.close)

        class FakeSubprocess:
            DEVNULL = subprocess.DEVNULL
            calls = []
            def call(self, args):
                self.calls.append(args)
                return 1
            def check_call(self, args):
                self.calls.append(args)
            def check_output(self, args, stderr=None):
                self.calls.append(args)
                return b'0\n'

        node_bin = shutil.which('node')
        # Verify legacy single-recipient migration path of install() before applying 3 contacts.
        migrated_summary = installer.install(
            Path(__file__).parents[1], Path('/usr/bin/podman'), Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, contacts_config=None, check_root=False,
            run_cmd=FakeSubprocess(), chown_fn=lambda *a: None,
        )
        self.assertEqual(len(migrated_summary['contacts']), 1)

        summary = installer.install(
            Path(__file__).parents[1], Path('/usr/bin/podman'), Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, contacts_config=contacts_file, check_root=False,
            run_cmd=FakeSubprocess(), chown_fn=lambda *a: None,
        )
        self.assertEqual(len(summary['contacts']), 3)
        self.assertEqual((wechat_dir / 'config/config.json').read_text(encoding='utf-8'), original_gamma_cfg)
        self.assertTrue((wechat_dir / 'config/octopus-contacts.json').is_file())
        installed_policy = json.loads(policy_path.read_text(encoding='utf-8'))
        self.assertEqual(len(installed_policy['wechatContacts']), 3)
        self.assertEqual(installed_policy['wechatContactsConfig'], str(wechat_dir / 'config/octopus-contacts.json'))
        self.assertNotIn('wechatRecipientAlias', installed_policy)

        # Cross-runtime check: Node broker prepares & queues a job for Friend 3; Python worker validates & executes it.
        with closing(sqlite3.connect(ingress_db)) as conn:
            conn.execute('CREATE TABLE u_im_ingress_operators(key TEXT PRIMARY KEY, value TEXT)')
            conn.execute('CREATE TABLE u_im_ingress_deliveries(key TEXT PRIMARY KEY, value TEXT)')
            conn.execute('INSERT INTO u_im_ingress_operators VALUES(?, ?)', (binding_key, json.dumps({'revokedAt': None, 'operatorId': 'owner', 'targetTenantId': 'octopus'})))
            now_iso = datetime.now(ZoneInfo('UTC')).isoformat()
            conn.execute('INSERT INTO u_im_ingress_deliveries VALUES(?, ?)', ('d1', json.dumps({
                'status': 'in_flight', 'operatorId': 'owner', 'targetTenantId': 'octopus', 'requestId': 'req-e2e',
                'prompt': '微信发给阿明-上海：跨语言核验', 'receivedAt': now_iso,
                'deliveryKey': json.dumps(['feishu', 'app', 'org', 'chat', 'owner', 'ev1'], separators=(',', ':')),
            })))
            conn.commit()

        script = (
            f"import {{ readPolicy, validatePolicy }} from {json.dumps((Path(__file__).parents[1] / 'policy.mjs').as_uri())};"
            f"import {{ Operations }} from {json.dumps((Path(__file__).parents[1] / 'operations.mjs').as_uri())};"
            f"import {{ perform, dispatch }} from {json.dumps((Path(__file__).parents[1] / 'broker.mjs').as_uri())};"
            "import { readFileSync } from 'node:fs';"
            f"const p = validatePolicy(JSON.parse(readFileSync({json.dumps(str(policy_path))}, 'utf8')));"
            f"const j = new Operations(p, {json.dumps(str(policy_path))}, perform);"
            "const op = j.prepare({ kind: 'wechat_send', payload: { recipient: '阿明-上海', text: '跨语言核验' } });"
            "const run = await j.run(op.id);"
            "console.log(JSON.stringify({ id: op.id, state: run.state }));"
            "j.close();"
        )
        node_out = json.loads(subprocess.check_output([node_bin, '--input-type=module', '-e', script], text=True))
        self.assertEqual(node_out['state'], 'queued')
        queued_path = wechat_dir / 'adhoc/outbox' / (node_out['id'] + '.json')
        installed_cfg = json.loads((wechat_dir / 'config/config.json').read_text(encoding='utf-8'))
        worker_res = worker.process_job(
            queued_path, wechat_dir, installed_cfg, Queue, run_one, MultiContactDesktop, trusted_uid=os.getuid(),
        )
        self.assertEqual(worker_res['state'], 'sent_verified')
        self.assertEqual(Desktop.submissions[-1], '跨语言核验')

    def test_late_install_failure_rolls_back_loaded_broker_and_worker_without_touching_unrelated_services(self):
        import shutil
        import socket
        import subprocess

        deploy_root = self.root / 'r'
        wechat_dir = deploy_root / 'wechat'
        release_root = deploy_root / 'releases-root'
        backup_root = deploy_root / 'backups'
        state_dir = deploy_root / 'state'
        policy_path = deploy_root / 'etc/octopus/operations.json'
        socket_path = self.root / 'b.sock'
        ingress_db = deploy_root / 'ingress/state/ingress.sqlite'
        ingress_profile = deploy_root / 'ingress/cordis.patch.yml'
        agent_profile = deploy_root / 'agent/profile'
        unit_dir = deploy_root / 'systemd'
        source_v1 = deploy_root / 'src-v1'
        source_v2 = deploy_root / 'src-v2'
        for d in [wechat_dir / 'config/contacts/friend-2', wechat_dir / 'config/contacts/friend-3',
                  wechat_dir / 'receipts', ingress_profile.parent, agent_profile, ingress_db.parent,
                  socket_path.parent, source_v1, source_v2]:
            d.mkdir(parents=True, exist_ok=True)
        for rel, body in [
            ('config/account.png', b'acc'), ('config/own-chat.png', b'own'),
            ('config/octopus-friend-label.png', b'lbl'),
            ('config/recipient.png', b'f1s'), ('config/recipient-sidebar.png', b'f1sb'), ('config/recipient-card.png', b'f1c'),
            ('config/contacts/friend-2/recipient.png', b'f2s'), ('config/contacts/friend-2/recipient-sidebar.png', b'f2sb'), ('config/contacts/friend-2/recipient-card.png', b'f2c'),
            ('config/contacts/friend-3/recipient.png', b'f3s'), ('config/contacts/friend-3/recipient-sidebar.png', b'f3sb'), ('config/contacts/friend-3/recipient-card.png', b'f3c'),
        ]:
            (wechat_dir / rel).write_bytes(body)
        (wechat_dir / 'config/config.json').write_text(json.dumps({
            'enabled': True, 'moments_enabled': True, 'direct_enabled': True, 'account_id': 'own-id', 'visibility': 'private',
            'account_anchor': '/home/wechat/.gamma-moments/config/account.png',
            'own_chat_anchor': '/home/wechat/.gamma-moments/config/own-chat.png',
            'recipient_id': 'wx-1', 'recipient_query': '阿明',
            'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
            'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
            'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
        }))
        binding_key = json.dumps(['feishu', 'app', 'org', 'chat', 'owner'], separators=(',', ':'))
        ingress_profile.write_text(f"operatorBindings: {json.dumps([{'bindingKey': binding_key, 'operatorId': 'owner', 'targetTenantId': 'octopus'}])}\n")
        (agent_profile / 'cordis.patch.yml').write_text(json.dumps([{'id': 'im-runtime', 'inject': ['agents']}]))
        (agent_profile / 'package.json').write_text(json.dumps({'name': 'octopus-profile', 'dependencies': {}}))

        src_dir = Path(__file__).parents[1]
        for fname in installer.RELEASE_FILES:
            content = (src_dir / fname).read_text(encoding='utf-8')
            (source_v1 / fname).write_text(content, encoding='utf-8')
            # Produce a distinct v2 candidate revision with identifiable broker/worker markers.
            v2_content = content
            if fname == 'broker.mjs':
                v2_content += '\n// CANDIDATE_V2_BROKER\n'
            elif fname == 'wechat_worker.py':
                v2_content += '\n# CANDIDATE_V2_WORKER\n'
            (source_v2 / fname).write_text(v2_content, encoding='utf-8')

        contacts_v1 = deploy_root / 'contacts-v1.json'
        contacts_v1.write_text(json.dumps([
            {'id': 'friend-1', 'type': 'friend', 'alias': 'v1-friend-1', 'display_name': '阿明', 'enabled': True, 'direct_enabled': True, 'legacy': True,
             'recipient_id': 'wx-1', 'recipient_query': '阿明',
             'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
             'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
             'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png'},
        ]))
        contacts_v2 = deploy_root / 'contacts-v2.json'
        contacts_v2.write_text(json.dumps([
            {'id': 'friend-1', 'type': 'friend', 'alias': 'v2-friend-1', 'display_name': '阿明', 'enabled': True, 'direct_enabled': True, 'legacy': True,
             'recipient_id': 'wx-1', 'recipient_query': '阿明',
             'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
             'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
             'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png'},
            {'id': 'friend-2', 'type': 'friend', 'alias': 'v2-friend-2', 'display_name': '阿强', 'enabled': True, 'direct_enabled': True,
             'recipient_id': 'wx-2', 'recipient_query': '阿强',
             'recipient_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient.png',
             'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-sidebar.png',
             'recipient_card_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-card.png'},
        ]))

        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(socket_path))
        srv.listen(8)
        self.addCleanup(srv.close)

        class StateAwareSystemctl:
            DEVNULL = subprocess.DEVNULL

            def __init__(self):
                self.loaded_units = {
                    'gamma-wechat-moments-worker.service': 'gamma-native',
                    'octopus-ingress.service': 'orig-ingress',
                    'gamma-wechat-login.service': 'orig-login',
                }
                self.services = {
                    'octopus-operations.service': {'active': False, 'enabled': False, 'live': None, 'starts': 0, 'restarts': 0, 'stops': 0},
                    'gamma-wechat-moments-worker.service': {'active': True, 'enabled': True, 'live': {'worker_code': 'pre-install'}, 'starts': 0, 'restarts': 0, 'stops': 0},
                    'octopus-ingress.service': {'active': True, 'enabled': True, 'live': {'ingress': 'orig'}, 'starts': 0, 'restarts': 0, 'stops': 0},
                    'gamma-wechat-login.service': {'active': True, 'enabled': True, 'live': {'login': 'orig'}, 'starts': 0, 'restarts': 0, 'stops': 0},
                }
                self.fail_check_output = False
                self.fail_recovery_unit = None
                self.armed_recovery_unit = None
                self.stop_stops_then_fails = False
                self.stop_fails_before_affecting = False
                self.ingress_restart_stops_then_fails = False
                self.ingress_restart_fails_before_affecting = False

            def _snapshot_unit(self, name):
                if name == 'octopus-operations.service':
                    p = unit_dir / 'octopus-operations.service'
                    return p.read_text(encoding='utf-8') if p.exists() else None
                if name == 'gamma-wechat-moments-worker.service':
                    d = unit_dir / 'gamma-wechat-moments-worker.service.d/octopus-operations.conf'
                    return d.read_text(encoding='utf-8') if d.exists() else 'gamma-native'
                return self.loaded_units.get(name, '')

            def _capture_live_process(self, name):
                loaded = self.loaded_units.get(name)
                if loaded is None:
                    raise RuntimeError(f'Unit not loaded: {name}')
                if name == 'octopus-operations.service':
                    current_target = Path(os.readlink(release_root / 'current'))
                    broker_text = (current_target / 'broker.mjs').read_text(encoding='utf-8')
                    pol = json.loads(policy_path.read_text(encoding='utf-8'))
                    sidecar = json.loads((wechat_dir / 'config/octopus-contacts.json').read_text(encoding='utf-8'))
                    return {
                        'unit_text': loaded,
                        'release': current_target.name,
                        'is_v2_code': 'CANDIDATE_V2_BROKER' in broker_text,
                        'policy_aliases': [c['alias'] for c in pol['wechatContacts']],
                        'sidecar_aliases': [c['alias'] for c in sidecar['contacts']],
                    }
                if name == 'gamma-wechat-moments-worker.service':
                    if loaded == 'gamma-native':
                        return {'mode': 'gamma-native'}
                    wrapper_text = (wechat_dir / 'runtime/octopus_ops_worker.py').read_text(encoding='utf-8')
                    sidecar = json.loads((wechat_dir / 'config/octopus-contacts.json').read_text(encoding='utf-8'))
                    return {
                        'dropin_text': loaded,
                        'is_v2_code': 'CANDIDATE_V2_WORKER' in wrapper_text,
                        'sidecar_aliases': [c['alias'] for c in sidecar['contacts']],
                    }
                if name == 'octopus-ingress.service':
                    patch_text = (agent_profile / 'cordis.patch.yml').read_text(encoding='utf-8')
                    pkg_text = (agent_profile / 'package.json').read_text(encoding='utf-8')
                    cur = Path(os.readlink(release_root / 'current')).name if (release_root / 'current').is_symlink() else None
                    return {'unit': name, 'patch_text': patch_text, 'pkg_text': pkg_text, 'current_release': cur}
                return {'unit': name}

            def call(self, args):
                if args[:2] == ['systemctl', 'is-active']:
                    return 0 if self.services[args[-1]]['active'] else 3
                if args[:2] == ['systemctl', 'is-enabled']:
                    return 0 if self.services[args[-1]]['enabled'] else 1
                return 0

            def check_call(self, args):
                if args[0] == 'install':
                    return
                assert args[0] == 'systemctl'
                op = args[1]
                if op == 'daemon-reload':
                    for uname in ('octopus-operations.service', 'gamma-wechat-moments-worker.service'):
                        snap = self._snapshot_unit(uname)
                        if snap is None:
                            self.loaded_units.pop(uname, None)
                        else:
                            self.loaded_units[uname] = snap
                    return
                unit = args[-1]
                if self.fail_recovery_unit == (op, unit):
                    raise RuntimeError(f'simulated recovery failure on {op} {unit}')
                svc = self.services[unit]
                if op == 'enable':
                    svc['enabled'] = True
                    if '--now' in args and not svc['active']:
                        svc['active'] = True
                        svc['starts'] += 1
                        svc['live'] = self._capture_live_process(unit)
                elif op == 'disable':
                    svc['enabled'] = False
                elif op == 'stop':
                    if unit == 'octopus-operations.service' and self.stop_fails_before_affecting:
                        self.stop_fails_before_affecting = False
                        raise RuntimeError('stop failed before stopping healthy broker')
                    svc['active'] = False
                    svc['stops'] += 1
                    svc['live'] = None
                    if unit == 'octopus-operations.service' and self.stop_stops_then_fails:
                        self.stop_stops_then_fails = False
                        raise RuntimeError('stop killed broker then exited nonzero')
                elif op == 'start':
                    # Matches real systemd: start on an already-active service is a no-op and keeps candidate process.
                    if not svc['active']:
                        svc['active'] = True
                        svc['starts'] += 1
                        svc['live'] = self._capture_live_process(unit)
                elif op == 'restart':
                    if unit == 'octopus-ingress.service' and self.ingress_restart_fails_before_affecting:
                        self.ingress_restart_fails_before_affecting = False
                        raise RuntimeError('ingress restart failed before stopping healthy ingress')
                    if unit == 'octopus-ingress.service' and self.ingress_restart_stops_then_fails:
                        self.ingress_restart_stops_then_fails = False
                        svc['active'] = False
                        svc['stops'] += 1
                        svc['live'] = None
                        raise RuntimeError('ingress stopped old process then candidate start failed')
                    svc['active'] = True
                    svc['restarts'] += 1
                    svc['live'] = self._capture_live_process(unit)

            def check_output(self, args, stderr=None):
                if self.fail_check_output:
                    if self.armed_recovery_unit is not None:
                        self.fail_recovery_unit = self.armed_recovery_unit
                    raise RuntimeError('simulated_ingress_query_failure')
                return b'0\n'

        node_bin = shutil.which('node')
        fake_sys = StateAwareSystemctl()
        v1_summary = installer.install(
            source_v1, Path('/usr/bin/podman'), Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, contacts_config=contacts_v1, check_root=False,
            run_cmd=fake_sys, chown_fn=lambda *a: None,
        )
        v1_rel = v1_summary['release']
        self.assertTrue(fake_sys.services['octopus-operations.service']['active'])
        self.assertTrue(fake_sys.services['gamma-wechat-moments-worker.service']['active'])
        v1_broker_unit = (unit_dir / 'octopus-operations.service').read_text(encoding='utf-8')
        v1_worker_dropin = (unit_dir / 'gamma-wechat-moments-worker.service.d/octopus-operations.conf').read_text(encoding='utf-8')
        ingress_restarts_before = fake_sys.services['octopus-ingress.service']['restarts']
        login_restarts_before = fake_sys.services['gamma-wechat-login.service']['restarts']

        # Inject failure right after candidate v2 broker & worker were restarted (during ingress check_output).
        fake_sys.fail_check_output = True
        with self.assertRaisesRegex(RuntimeError, 'simulated_ingress_query_failure'):
            installer.install(
                source_v2, Path('/usr/bin/podman'), Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, contacts_config=contacts_v2, check_root=False,
                run_cmd=fake_sys, chown_fn=lambda *a: None,
            )

        # Assert BOTH on-disk state AND effective live loaded generations rolled back to v1.
        self.assertEqual(Path(os.readlink(release_root / 'current')).name, v1_rel)
        self.assertEqual((unit_dir / 'octopus-operations.service').read_text(encoding='utf-8'), v1_broker_unit)
        self.assertEqual((unit_dir / 'gamma-wechat-moments-worker.service.d/octopus-operations.conf').read_text(encoding='utf-8'), v1_worker_dropin)
        live_broker = fake_sys.services['octopus-operations.service']['live']
        live_worker = fake_sys.services['gamma-wechat-moments-worker.service']['live']
        self.assertEqual(live_broker['release'], v1_rel)
        self.assertFalse(live_broker['is_v2_code'])
        self.assertEqual(live_broker['policy_aliases'], ['v1-friend-1'])
        self.assertEqual(live_broker['sidecar_aliases'], ['v1-friend-1'])
        self.assertEqual(live_broker['unit_text'], v1_broker_unit)
        self.assertFalse(live_worker['is_v2_code'])
        self.assertEqual(live_worker['sidecar_aliases'], ['v1-friend-1'])
        self.assertEqual(live_worker['dropin_text'], v1_worker_dropin)
        # Unrelated services were never touched or restarted.
        self.assertEqual(fake_sys.services['octopus-ingress.service']['restarts'], ingress_restarts_before)
        self.assertEqual(fake_sys.services['octopus-ingress.service']['stops'], 0)
        self.assertEqual(fake_sys.services['gamma-wechat-login.service']['restarts'], login_restarts_before)
        self.assertEqual(fake_sys.services['gamma-wechat-login.service']['stops'], 0)

        # Verify rollback failure preserves and surfaces BOTH the install failure and restoration failure.
        fake_sys.armed_recovery_unit = ('restart', 'gamma-wechat-moments-worker.service')
        with self.assertRaises(RuntimeError) as ctx:
            installer.install(
                source_v2, Path('/usr/bin/podman'), Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, contacts_config=contacts_v2, check_root=False,
                run_cmd=fake_sys, chown_fn=lambda *a: None,
            )
        self.assertIn('simulated_ingress_query_failure', str(ctx.exception))
        self.assertIn('rollback failed', str(ctx.exception))
        self.assertIn('gamma-wechat-moments-worker.service', str(ctx.exception))

        # Recover from the intentional worker rollback failure so subsequent cases start from clean v1 active state.
        fake_sys.fail_recovery_unit = None
        fake_sys.fail_check_output = False
        installer.install(
            source_v1, Path('/usr/bin/podman'), Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, contacts_config=contacts_v1, check_root=False,
            run_cmd=fake_sys, chown_fn=lambda *a: None,
        )
        v1_patch_text = (agent_profile / 'cordis.patch.yml').read_text(encoding='utf-8')
        v1_pkg_text = (agent_profile / 'package.json').read_text(encoding='utf-8')

        # Bug 1 regression: initial `systemctl stop` stops old broker (active=False, live=None) then fails -> must restore old broker!
        worker_restarts_before_stop = fake_sys.services['gamma-wechat-moments-worker.service']['restarts']
        ingress_restarts_before_stop = fake_sys.services['octopus-ingress.service']['restarts']
        fake_sys.stop_stops_then_fails = True
        with self.assertRaisesRegex(RuntimeError, 'stop killed broker then exited nonzero'):
            installer.install(
                source_v2, Path('/usr/bin/podman'), Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, contacts_config=contacts_v2, check_root=False,
                run_cmd=fake_sys, chown_fn=lambda *a: None,
            )
        self.assertTrue(fake_sys.services['octopus-operations.service']['active'])
        self.assertEqual(fake_sys.services['octopus-operations.service']['live']['release'], v1_rel)
        self.assertFalse(fake_sys.services['octopus-operations.service']['live']['is_v2_code'])
        self.assertEqual(fake_sys.services['gamma-wechat-moments-worker.service']['restarts'], worker_restarts_before_stop)
        self.assertEqual(fake_sys.services['octopus-ingress.service']['restarts'], ingress_restarts_before_stop)

        # Pre-stop failure that never stopped the healthy broker must NOT disrupt broker, worker, or ingress.
        broker_restarts_before = fake_sys.services['octopus-operations.service']['restarts']
        broker_starts_before = fake_sys.services['octopus-operations.service']['starts']
        fake_sys.stop_fails_before_affecting = True
        with self.assertRaisesRegex(RuntimeError, 'stop failed before stopping healthy broker'):
            installer.install(
                source_v2, Path('/usr/bin/podman'), Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, contacts_config=contacts_v2, check_root=False,
                run_cmd=fake_sys, chown_fn=lambda *a: None,
            )
        self.assertTrue(fake_sys.services['octopus-operations.service']['active'])
        self.assertEqual(fake_sys.services['octopus-operations.service']['restarts'], broker_restarts_before)
        self.assertEqual(fake_sys.services['octopus-operations.service']['starts'], broker_starts_before)

        # Bug 2 regression: `systemctl restart octopus-ingress.service` stops old ingress then candidate start fails -> must restore v1 ingress too!
        fake_sys.ingress_restart_stops_then_fails = True
        with self.assertRaisesRegex(RuntimeError, 'ingress stopped old process then candidate start failed'):
            installer.install(
                source_v2, Path('/usr/bin/podman'), Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, contacts_config=contacts_v2, check_root=False,
                run_cmd=fake_sys, chown_fn=lambda *a: None,
            )
        self.assertTrue(fake_sys.services['octopus-ingress.service']['active'])
        live_ingress = fake_sys.services['octopus-ingress.service']['live']
        self.assertEqual(live_ingress['current_release'], v1_rel)
        self.assertEqual(live_ingress['patch_text'], v1_patch_text)
        self.assertEqual(live_ingress['pkg_text'], v1_pkg_text)
        self.assertFalse(fake_sys.services['octopus-operations.service']['live']['is_v2_code'])
        self.assertFalse(fake_sys.services['gamma-wechat-moments-worker.service']['live']['is_v2_code'])
        self.assertEqual(fake_sys.services['gamma-wechat-login.service']['stops'], 0)

        # Ingress restart failure before affecting healthy ingress must roll back broker/worker without needlessly restarting healthy ingress.
        ingress_restarts_before_pre = fake_sys.services['octopus-ingress.service']['restarts']
        fake_sys.ingress_restart_fails_before_affecting = True
        with self.assertRaisesRegex(RuntimeError, 'ingress restart failed before stopping healthy ingress'):
            installer.install(
                source_v2, Path('/usr/bin/podman'), Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, contacts_config=contacts_v2, check_root=False,
                run_cmd=fake_sys, chown_fn=lambda *a: None,
            )
        self.assertTrue(fake_sys.services['octopus-ingress.service']['active'])
        self.assertEqual(fake_sys.services['octopus-ingress.service']['restarts'], ingress_restarts_before_pre)
        self.assertFalse(fake_sys.services['octopus-operations.service']['live']['is_v2_code'])
        self.assertFalse(fake_sys.services['gamma-wechat-moments-worker.service']['live']['is_v2_code'])

        # Fresh install late failure (when broker and worker were previously inactive) must stop/disable candidates.
        fresh_root = deploy_root / 'f'
        fresh_wechat = fresh_root / 'wechat'
        fresh_rel = fresh_root / 'releases'
        fresh_bak = fresh_root / 'backups'
        fresh_state = fresh_root / 'state'
        fresh_policy = fresh_root / 'etc/octopus/operations.json'
        fresh_sock = self.root / 'f.sock'
        fresh_units = fresh_root / 'systemd'
        fresh_agent = fresh_root / 'agent/profile'
        for d in [fresh_wechat / 'config', fresh_wechat / 'receipts', fresh_sock.parent, fresh_agent]:
            d.mkdir(parents=True, exist_ok=True)
        for rel, body in [('config/account.png', b'acc'), ('config/own-chat.png', b'own'), ('config/octopus-friend-label.png', b'lbl'),
                          ('config/recipient.png', b'f1s'), ('config/recipient-sidebar.png', b'f1sb'), ('config/recipient-card.png', b'f1c')]:
            (fresh_wechat / rel).write_bytes(body)
        (fresh_wechat / 'config/config.json').write_text((wechat_dir / 'config/config.json').read_text(encoding='utf-8'))
        (fresh_agent / 'cordis.patch.yml').write_text(json.dumps([{'id': 'im-runtime', 'inject': ['agents']}]))
        (fresh_agent / 'package.json').write_text(json.dumps({'name': 'octopus-profile', 'dependencies': {}}))
        fresh_srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        fresh_srv.bind(str(fresh_sock))
        fresh_srv.listen(4)
        self.addCleanup(fresh_srv.close)

        class FreshFailSystemctl(StateAwareSystemctl):
            def __init__(self):
                super().__init__()
                self.services['gamma-wechat-moments-worker.service']['active'] = False
                self.services['gamma-wechat-moments-worker.service']['live'] = None
                self.fail_check_output = True

            def _snapshot_unit(self, name):
                if name == 'octopus-operations.service':
                    p = fresh_units / 'octopus-operations.service'
                    return p.read_text(encoding='utf-8') if p.exists() else None
                if name == 'gamma-wechat-moments-worker.service':
                    d = fresh_units / 'gamma-wechat-moments-worker.service.d/octopus-operations.conf'
                    return d.read_text(encoding='utf-8') if d.exists() else 'gamma-native'
                return self.loaded_units.get(name, '')

            def _capture_live_process(self, name):
                loaded = self.loaded_units.get(name)
                if loaded is None:
                    raise RuntimeError(f'Unit not loaded: {name}')
                return {'unit': name, 'loaded': loaded}

        fresh_sys = FreshFailSystemctl()
        with self.assertRaisesRegex(RuntimeError, 'simulated_ingress_query_failure'):
            installer.install(
                source_v2, Path('/usr/bin/podman'), Path(node_bin),
                wechat_dir=fresh_wechat, release_root=fresh_rel, backup_root_base=fresh_bak,
                state_dir=fresh_state, policy_path=fresh_policy, socket_path=fresh_sock,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=fresh_agent,
                unit_dir=fresh_units, contacts_config=contacts_v1, check_root=False,
                run_cmd=fresh_sys, chown_fn=lambda *a: None,
            )
        self.assertFalse(fresh_sys.services['octopus-operations.service']['active'])
        self.assertFalse(fresh_sys.services['octopus-operations.service']['enabled'])
        self.assertFalse(fresh_sys.services['gamma-wechat-moments-worker.service']['active'])
        self.assertIsNone(fresh_sys.services['octopus-operations.service']['live'])
        self.assertIsNone(fresh_sys.services['gamma-wechat-moments-worker.service']['live'])
        self.assertFalse((fresh_rel / 'current').exists())
        self.assertFalse((fresh_policy.exists()))
        self.assertFalse((fresh_wechat / 'config/octopus-contacts.json').exists())
        self.assertFalse((fresh_units / 'octopus-operations.service').exists())


class Task2AdapterFailClosedTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for sub in ['adhoc/outbox', 'adhoc/receipts', 'outbox', 'receipts', 'config/groups/g1']:
            (self.root / sub).mkdir(parents=True, exist_ok=True)
        for rel, body in [
            ('config/account.png', b'acc'),
            ('config/own-chat.png', b'own'),
            ('config/octopus-friend-label.png', b'lbl'),
            ('config/recipient.png', b'f1s'),
            ('config/recipient-sidebar.png', b'f1sb'),
            ('config/recipient-card.png', b'f1c'),
            ('config/groups/g1/group-search.png', b'gs'),
            ('config/groups/g1/member-1.png', b'm1'),
        ]:
            (self.root / rel).write_bytes(body)
        self.config_path = self.root / 'config/config.json'
        self.contacts_path = self.root / 'config/octopus-contacts.json'
        self.config = {
            'enabled': True, 'moments_enabled': True, 'direct_enabled': True,
            'account_id': 'own-id', 'account_anchor': '/home/wechat/.gamma-moments/config/account.png',
            'own_chat_anchor': '/home/wechat/.gamma-moments/config/own-chat.png',
            'recipient_id': 'wx-1', 'recipient_query': '阿明',
            'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
            'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
            'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
        }
        self.sidecar = {
            'schema_version': 1,
            'account_sha256': worker.sha('own-id'),
            'adhoc_moments': {'enabled': True, 'default_visibility': 'private'},
            'contacts': [
                {
                    'id': 'friend-1', 'type': 'friend', 'alias': '好友一', 'display_name': '阿明',
                    'enabled': True, 'direct_enabled': True, 'legacy': True,
                    'recipient_id': 'wx-1', 'recipient_query': '阿明',
                    'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
                    'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
                    'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
                },
                {
                    'id': 'group-1', 'type': 'group', 'alias': '策略群', 'display_name': '策略群全称',
                    'enabled': True, 'group_enabled': True,
                    'group_id': 'wx-g1', 'group_name': '策略群全称', 'group_query': '策略群全称',
                    'group_anchor': '/home/wechat/.gamma-moments/config/groups/g1/group-search.png',
                    'group_member_anchors': ['/home/wechat/.gamma-moments/config/groups/g1/member-1.png'],
                    'group_name_read_mode': 'native_copy',
                },
            ],
        }
        self.config_path.write_text(json.dumps(self.config), encoding='utf-8')
        self.contacts_path.write_text(json.dumps(self.sidecar), encoding='utf-8')
        self.contacts_path.chmod(0o640)

    def test_missing_task2_adapters_refuse_group_and_adhoc_jobs_fail_closed(self):
        now = int(time.time() * 1000)
        g_id = 'a1' * 12
        g_binding = worker.compute_group_binding_sha256(self.root, self.config, self.sidecar['contacts'][1])
        g_path = self.root / 'adhoc/outbox' / f'{g_id}.json'
        g_path.write_text(json.dumps({
            'schema_version': 2, 'channel': 'group', 'kind': 'send', 'job_id': g_id,
            'account_sha256': worker.sha('own-id'), 'contact_id': 'group-1', 'contact_type': 'group',
            'group_id': 'wx-g1', 'group_name': '策略群全称', 'group_alias': '策略群',
            'binding_sha256': g_binding, 'text': 'group msg', 'text_sha256': worker.sha('group msg'),
            'created_at': now, 'expires_at': now + 600000, 'confirmation_request_id': 'req-1',
        }), encoding='utf-8')
        g_path.chmod(0o640)
        g_res = worker.process_job(
            g_path, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            group_desktop_class=False,
        )
        self.assertEqual(g_res['state'], 'refused')
        self.assertEqual(g_res['reason'], 'gamma_task2_adapter_unavailable')

        if not HAS_PHASE2_GAMMA:
            m_id = 'b1' * 12
            m_path = self.root / 'adhoc/outbox' / f'{m_id}.json'
            m_path.write_text(json.dumps({
                'schema_version': 2, 'channel': 'adhoc_moments', 'kind': 'publish', 'job_id': m_id,
                'account_sha256': worker.sha('own-id'), 'visibility': 'private', 'binding_sha256': '0' * 64,
                'text': 'moment', 'text_sha256': worker.sha('moment'), 'trade_date': '2026-11-21',
                'created_at': now, 'expires_at': now + 600000, 'confirmation_request_id': 'req-2',
            }), encoding='utf-8')
            m_path.chmod(0o640)
            m_res = worker.process_job(
                m_path, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                adhoc_desktop_class=False,
            )
            self.assertEqual(m_res['state'], 'refused')
            self.assertEqual(m_res['reason'], 'gamma_task2_adapter_unavailable')
            with self.assertRaisesRegex(RuntimeError, 'Gamma Task 2 desktop adapters are unavailable'):
                installer.verify_gamma_task2_adapters(in_process=True)

    def test_installer_fails_before_service_stop_when_task2_enabled_and_adapter_absent(self):
        import shutil
        from types import SimpleNamespace
        deploy_root = self.root / 'deploy'
        wechat_dir = deploy_root / 'wechat'
        source_dir = deploy_root / 'src'
        for d in [wechat_dir / 'config/groups/g1', wechat_dir / 'receipts', source_dir, deploy_root / 'agent']:
            d.mkdir(parents=True, exist_ok=True)
        for fn in installer.RELEASE_FILES:
            shutil.copyfile(Path(__file__).parents[1] / fn, source_dir / fn)
        for rel, body in [
            ('config/account.png', b'acc'), ('config/own-chat.png', b'own'),
            ('config/octopus-friend-label.png', b'lbl'), ('config/recipient.png', b'f1s'),
            ('config/recipient-sidebar.png', b'f1sb'), ('config/recipient-card.png', b'f1c'),
            ('config/groups/g1/group-search.png', b'gs'), ('config/groups/g1/member-1.png', b'm1'),
        ]:
            (wechat_dir / rel).write_bytes(body)
        (wechat_dir / 'config/config.json').write_text(json.dumps(self.config), encoding='utf-8')
        ingress_profile = deploy_root / 'ingress.yml'
        ingress_profile.write_text('operatorBindings: [{"bindingKey":"[\\"feishu\\",\\"a\\",\\"o\\",\\"c\\",\\"u\\"]","operatorId":"u","targetTenantId":"t"}]\n')
        (deploy_root / 'agent/cordis.patch.yml').write_text(json.dumps([{'id': 'im-runtime', 'inject': ['agents']}]))
        (deploy_root / 'agent/package.json').write_text(json.dumps({'name': 'p', 'dependencies': {}}))
        contacts_file = deploy_root / 'contacts.json'
        contacts_file.write_text(json.dumps(self.sidecar['contacts']), encoding='utf-8')

        calls = []
        fake_cmd = SimpleNamespace(
            DEVNULL=0,
            call=lambda cmd, **kw: calls.append(('call', cmd)) or 1,
            check_call=lambda cmd, **kw: calls.append(('check_call', cmd)) or 0,
            check_output=lambda cmd, **kw: calls.append(('check_output', cmd)) or b'0\n',
        )

        def fail_probe():
            raise RuntimeError('Gamma Task 2 desktop adapters are unavailable: missing GroupDesktop')

        with self.assertRaisesRegex(RuntimeError, 'Gamma Task 2 desktop adapters are unavailable'):
            installer.install(
                source_dir, deploy_root / 'runtime', Path(shutil.which('node') or '/usr/bin/node'),
                wechat_dir=wechat_dir, release_root=deploy_root / 'releases',
                backup_root_base=deploy_root / 'bak', state_dir=deploy_root / 'state',
                policy_path=deploy_root / 'policy.json', socket_path=self.root / 's.sock',
                ingress_db=deploy_root / 'ingress.sqlite', ingress_profile=ingress_profile,
                agent_profile=deploy_root / 'agent', unit_dir=deploy_root / 'units',
                contacts_config=contacts_file, check_task2_adapters=fail_probe,
                check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(calls, [])

        # Default install without adhoc_moments_config leaves adhocMoments disabled even when contacts has only friends.
        import socket as pysocket
        srv_ok = pysocket.socket(pysocket.AF_UNIX, pysocket.SOCK_STREAM)
        srv_ok.bind(str(self.root / 's.sock'))
        srv_ok.listen(2)
        self.addCleanup(srv_ok.close)

        friends_only = deploy_root / 'friends.json'
        friends_only.write_text(json.dumps([self.sidecar['contacts'][0]]), encoding='utf-8')
        summary = installer.install(
            source_dir, deploy_root / 'runtime', Path(shutil.which('node') or '/usr/bin/node'),
            wechat_dir=wechat_dir, release_root=deploy_root / 'releases',
            backup_root_base=deploy_root / 'bak', state_dir=deploy_root / 'state',
            policy_path=deploy_root / 'policy.json', socket_path=self.root / 's.sock',
            ingress_db=deploy_root / 'ingress.sqlite', ingress_profile=ingress_profile,
            agent_profile=deploy_root / 'agent', unit_dir=deploy_root / 'units',
            contacts_config=friends_only, check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
        )
        self.assertEqual(summary['adhocMoments'], {'enabled': False})
        self.assertFalse(any(c['type'] == 'group' for c in summary['contacts']))
        written_sidecar = json.loads((wechat_dir / 'config/octopus-contacts.json').read_text(encoding='utf-8'))
        self.assertEqual(written_sidecar['adhoc_moments'], {'enabled': False, 'default_visibility': None})


@unittest.skipUnless(HAS_PHASE2_GAMMA, 'Gamma Task 2 GroupDesktop/AdhocMomentsDesktop adapters required')
class GroupAndAdhocMomentsTests(unittest.TestCase):
    def setUp(self):
        from PIL import Image
        import numpy as np
        self.Image = Image
        self.np = np
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for sub in [
            'adhoc/outbox', 'adhoc/receipts/evidence', 'outbox', 'receipts/evidence',
            'config/groups/ops-group', 'config/groups/bak-group', 'assets',
        ]:
            (self.root / sub).mkdir(parents=True, exist_ok=True)

        def write_pattern(path, seed, size=(24, 24)):
            rng = np.random.default_rng(seed)
            arr = rng.integers(0, 255, (size[1], size[0], 3), dtype=np.uint8)
            Image.fromarray(arr).save(path)
            return arr

        self.assets = self.root / 'assets'
        for name, seed, sz in [
            ('account.png', 1, (16, 16)), ('camera.png', 2, (16, 16)), ('placeholder.png', 3, (16, 16)),
            ('publish.png', 4, (16, 16)), ('private-choice.png', 5, (16, 16)), ('friends-choice.png', 6, (16, 16)),
            ('confirm.png', 7, (16, 16)), ('private-badge.png', 8, (16, 16)), ('friends-badge.png', 9, (16, 16)),
            ('public-badge.png', 10, (16, 16)), ('own-delete.png', 11, (15, 16)),
            ('chat-search.png', 12, (20, 20)), ('chat-contacts.png', 13, (20, 20)),
            ('chat-groups.png', 14, (20, 20)), ('chat-network.png', 15, (20, 20)),
            ('chat-details.png', 16, (20, 20)), ('chat-tools.png', 17, (20, 20)),
            ('chat-send.png', 18, (20, 20)), ('chat-copy.png', 19, (20, 20)), ('copy.png', 20, (20, 20)),
        ]:
            arr = write_pattern(self.assets / name, seed, size=sz)
            if name == 'own-delete.png':
                self.own_delete = arr

        self.tpl_account = write_pattern(self.root / 'config/account.png', 31, size=(16, 16))
        self.tpl_own = write_pattern(self.root / 'config/own-chat.png', 32, size=(24, 24))
        write_pattern(self.root / 'config/octopus-friend-label.png', 33, size=(16, 16))
        write_pattern(self.root / 'config/recipient.png', 34, size=(24, 24))
        write_pattern(self.root / 'config/recipient-sidebar.png', 35, size=(24, 24))
        write_pattern(self.root / 'config/recipient-card.png', 36, size=(24, 24))
        self.tpl_group_search = write_pattern(self.root / 'config/groups/ops-group/group-search.png', 41, size=(24, 24))
        self.tpl_m1 = write_pattern(self.root / 'config/groups/ops-group/member-1.png', 42, size=(24, 24))
        self.tpl_m2 = write_pattern(self.root / 'config/groups/ops-group/member-2.png', 43, size=(24, 24))
        self.tpl_group_name = write_pattern(self.root / 'config/groups/ops-group/group-name.png', 44, size=(90, 18))
        self.tpl_bak_search = write_pattern(self.root / 'config/groups/bak-group/group-search.png', 51, size=(24, 24))
        self.tpl_bak_m1 = write_pattern(self.root / 'config/groups/bak-group/member-1.png', 52, size=(24, 24))

        self.config_path = self.root / 'config/config.json'
        self.contacts_path = self.root / 'config/octopus-contacts.json'
        self.config = {
            'enabled': True, 'moments_enabled': False, 'direct_enabled': True,
            'account_id': 'own-id',
            'evidence_dir': str(self.root / 'receipts/evidence'),
            'adhoc_evidence_dir': str(self.root / 'adhoc/receipts/evidence'),
            'account_anchor': '/home/wechat/.gamma-moments/config/account.png',
            'own_chat_anchor': '/home/wechat/.gamma-moments/config/own-chat.png',
            'recipient_id': 'wx-1', 'recipient_query': '阿明',
            'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
            'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
            'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
        }
        self.sidecar = {
            'schema_version': 1,
            'account_sha256': worker.sha('own-id'),
            'adhoc_moments': {'enabled': True, 'default_visibility': 'private'},
            'contacts': [
                {
                    'id': 'friend-1', 'type': 'friend', 'alias': '好友一', 'display_name': '阿明',
                    'enabled': True, 'direct_enabled': True, 'legacy': True,
                    'recipient_id': 'wx-1', 'recipient_query': '阿明',
                    'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
                    'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
                    'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
                },
                {
                    'id': 'group-ops', 'type': 'group', 'alias': '策略投研群', 'display_name': '策略投研正式群',
                    'enabled': True, 'group_enabled': True,
                    'group_id': 'wx-group-ops', 'group_name': '策略投研正式群', 'group_query': '策略投研',
                    'group_anchor': '/home/wechat/.gamma-moments/config/groups/ops-group/group-search.png',
                    'group_member_anchors': [
                        '/home/wechat/.gamma-moments/config/groups/ops-group/member-1.png',
                        '/home/wechat/.gamma-moments/config/groups/ops-group/member-2.png',
                    ],
                    'group_name_anchor': '/home/wechat/.gamma-moments/config/groups/ops-group/group-name.png',
                    'group_name_read_mode': 'auto',
                },
                {
                    'id': 'group-bak', 'type': 'group', 'alias': '只读核验群', 'display_name': '备份观察群',
                    'enabled': True, 'group_enabled': False,
                    'group_id': 'wx-group-bak', 'group_name': '备份观察群', 'group_query': '备份观察群',
                    'group_anchor': '/home/wechat/.gamma-moments/config/groups/bak-group/group-search.png',
                    'group_member_anchors': [
                        '/home/wechat/.gamma-moments/config/groups/bak-group/member-1.png',
                    ],
                    'group_name_read_mode': 'native_copy',
                },
            ],
        }
        self.config_path.write_text(json.dumps(self.config), encoding='utf-8')
        self.contacts_path.write_text(json.dumps(self.sidecar), encoding='utf-8')
        self.contacts_path.chmod(0o640)

    def _make_group_desktop_cls(self, scenario=None):
        test_self = self
        active_scenario = scenario or {}

        class FakeGroupDesktop(GroupDesktop):
            submits = 0

            def __init__(inner_self, cfg):
                inner_self._clip = ''
                inner_self._draft = ''
                inner_self._stage = 'idle'
                inner_self._sent_bubbles = list(active_scenario.get('initial_bubbles', []))
                inner_self._right_click_target = None
                super().__init__(cfg, assets=test_self.assets, runner=inner_self._run_cmd)

            def identity(inner_self):
                return worker.sha(inner_self.config['account_id'])

            def window(inner_self, title):
                return {'id': 'win-main', 'title': '微信', 'box': (0, 0, 1280, 900)}

            def clipboard_set(inner_self, text):
                inner_self._clip = text

            def clipboard_get(inner_self):
                return inner_self._clip

            def _run_cmd(inner_self, *args, **kwargs):
                if args[:2] == ('xdotool', 'key') and args[2] == 'ctrl+v':
                    inner_self._draft = inner_self._clip
                elif args[:2] == ('xdotool', 'key') and args[2] == 'ctrl+c':
                    inner_self._clip = inner_self._draft
                return ''

            def visual_box(inner_self, window, name):
                boxes = {
                    'chat-search': (80, 40, 20, 20),
                    'chat-contacts': (80, 80, 40, 15),
                    'chat-groups': (80, 170, 40, 15),
                    'chat-network': (80, 320, 40, 15),
                    'chat-details': (1240, 40, 20, 20),
                    'chat-tools': (420, 760, 20, 20),
                    'chat-send': (1100, 860, 40, 20),
                    'chat-copy': (850, 200, 36, 20),
                }
                if name not in boxes:
                    raise UiUnavailable(f'visual_anchor_missing_{name}')
                return boxes[name]

            def icon_click(inner_self, window, name, *, hold=False):
                if name == 'chat-details':
                    inner_self._stage = 'chat' if inner_self._stage == 'details' else 'details'
                    return
                if name == 'copy':
                    inner_self._clip = inner_self.config['group_name']
                    return
                if name == 'chat-copy':
                    if inner_self._right_click_target is not None:
                        inner_self._clip = inner_self._right_click_target
                    return
                inner_self.visual_box(window, name)

            def _click_box(inner_self, window, box, *, button='1', hold=False):
                if button == '3':
                    bx, by, _, _ = box
                    inner_self._right_click_target = None
                    for bubble_y, bubble_text in inner_self._sent_bubbles:
                        if bubble_y <= by <= bubble_y + 45:
                            inner_self._right_click_target = bubble_text
                            break

            def _visual_pixels(inner_self, window):
                return inner_self.capture(window)[0]

            def capture(inner_self, window):
                img = test_self.np.full((900, 1280, 3), 248, dtype=test_self.np.uint8)
                rows = []
                is_bak = inner_self.config['group_id'] == 'wx-group-bak'
                search_tpl = test_self.tpl_bak_search if is_bak else test_self.tpl_group_search
                m1_tpl = test_self.tpl_bak_m1 if is_bak else test_self.tpl_m1
                img[200:224, 80:104] = search_tpl
                for by, btext in inner_self._sent_bubbles:
                    img[by:by + 40, 900:1200] = (149, 236, 105)
                    img[by:by + 24, 1220:1244] = test_self.tpl_own
                    rows.append({'text': btext, 'box': (920, by + 10, 140, 16)})
                if inner_self._stage == 'details':
                    img[40:900, 1000:1280] = 248
                    rows = [r for r in rows if r['box'][0] + r['box'][2] <= 1000]
                    if active_scenario.get('friend_sidebar'):
                        rows.extend([{'text': '朋友资料', 'box': (1040, 110, 60, 16)}, {'text': '微信号: person-1', 'box': (1040, 140, 110, 16)}])
                    else:
                        rows.extend([
                            {'text': '群聊名称', 'box': (1040, 110, 60, 16)},
                            {'text': '群公告', 'box': (1040, 210, 50, 16)},
                            {'text': inner_self.config['group_name'], 'box': (1040, 140, 110, 16)},
                        ])
                        if not is_bak:
                            img[138:156, 1040:1130] = test_self.tpl_group_name
                        img[260:284, 1040:1064] = m1_tpl
                        if len(inner_self.config.get('group_member_anchors', [])) == 2 and not active_scenario.get('missing_m2'):
                            img[260:284, 1100:1124] = test_self.tpl_m2
                if window['box'][2] != 1280:
                    hx, hy, hw, hh = window['box']
                    cropped_rows = [
                        {'text': r['text'], 'box': (r['box'][0] - hx, r['box'][1] - hy, r['box'][2], r['box'][3])}
                        for r in rows
                        if hx <= r['box'][0] and r['box'][0] + r['box'][2] <= hx + hw and hy <= r['box'][1] and r['box'][1] + r['box'][3] <= hy + hh
                    ]
                    return img[hy:hy + hh, hx:hx + hw], cropped_rows
                return img, rows

            def submit(inner_self):
                super().submit()
                FakeGroupDesktop.submits += 1
                next_y = max((y for y, _ in inner_self._sent_bubbles), default=120) + 90
                inner_self._sent_bubbles.append((next_y, inner_self._draft))
                inner_self._draft = ''

        return FakeGroupDesktop

    def _make_adhoc_desktop_cls(self, shared_posts, scenario=None):
        test_self = self
        active_scenario = scenario or {}

        class FakeAdhocDesktop(AdhocMomentsDesktop):
            submits = 0
            instances = []

            def __init__(inner_self, cfg):
                inner_self.posts = shared_posts
                inner_self.view = 'feed'
                inner_self._draft = ''
                inner_self._selected_visibility = active_scenario.get('default_visibility', 'private')
                inner_self._clip = ''
                inner_self._copied_in_feed = False
                super().__init__(cfg, assets=test_self.assets, runner=inner_self._run_cmd)
                FakeAdhocDesktop.instances.append(inner_self)

            def identity(inner_self):
                return worker.sha(inner_self.config['account_id'])

            def window(inner_self, title):
                w, h = active_scenario.get('window_size', (620, 800))
                return {'id': 'win-moments', 'title': '朋友圈', 'box': (330, 35, w, h)}

            def _moments(inner_self):
                return inner_self.window('朋友圈')

            def clipboard_set(inner_self, text):
                inner_self._clip = text

            def clipboard_get(inner_self):
                return inner_self._clip

            def _run_cmd(inner_self, *args, **kwargs):
                if args[:2] == ('xdotool', 'key') and args[2] == 'ctrl+v':
                    inner_self._draft = inner_self._clip
                elif args[:2] == ('xdotool', 'key') and args[2] == 'ctrl+c':
                    inner_self._clip = inner_self._draft
                return ''

            def visual_box(inner_self, window, name):
                return (189, 655, 117, 31) if name == 'publish' else (100, 100, 24, 24)

            def audience_box(inner_self, window, visibility):
                if visibility != inner_self._selected_visibility:
                    raise UiUnavailable('visual_anchor_missing')
                return (400, 612, 33, 17)

            def audience_row(inner_self, window):
                return (180, 610, 65, 18)

            def icon_click(inner_self, window, name, *, hold=False):
                if name == 'camera':
                    inner_self.view = 'composer'
                elif name == 'private-choice':
                    inner_self._selected_visibility = 'private'
                elif name == 'friends-choice':
                    inner_self._selected_visibility = 'friends'

            def text_click(inner_self, window, text, *, contains=False, button='1'):
                if text == '复制' and inner_self.view == 'feed' and inner_self.posts:
                    inner_self._copied_in_feed = True
                    inner_self._clip = inner_self.posts[0]['text']
                    return
                return super().text_click(window, text, contains=contains, button=button)

            def _click_box(inner_self, window, box, *, button='1', hold=False):
                if inner_self.view == 'feed' and button == '1' and inner_self._copied_in_feed and inner_self.posts and 60 <= box[1] <= 400:
                    inner_self.view = 'detail'

            def submit(inner_self):
                super().submit()
                FakeAdhocDesktop.submits += 1
                if active_scenario.get('silent_drop'):
                    inner_self.view = 'feed'
                    return
                if active_scenario.get('post_then_raise'):
                    inner_self.posts.insert(0, {
                        'text': inner_self._draft, 'own': True, 'visibility': inner_self._selected_visibility,
                        'time': '刚刚', 'tag': f'p-{FakeAdhocDesktop.submits}',
                    })
                    inner_self._draft = ''
                    inner_self.view = 'feed'
                    raise RuntimeError('crash_after_publish')
                inner_self.posts.insert(0, {
                    'text': inner_self._draft, 'own': True, 'visibility': inner_self._selected_visibility,
                    'time': '刚刚', 'tag': f'p-{FakeAdhocDesktop.submits}',
                })
                inner_self._draft = ''
                inner_self.view = 'feed'

            def capture(inner_self, window):
                w, h = window['box'][2], window['box'][3]
                img = test_self.np.full((h, w, 3), 248, dtype=test_self.np.uint8)
                rows = []
                if inner_self.view == 'composer':
                    rows.append({'text': '谁可以看', 'box': (180, 610, 65, 18)})
                    rows.append({'text': '发表', 'box': (189, 655, 117, 31)})
                    y = 110
                    for ln in [ln for ln in inner_self._draft.splitlines() if ln.strip()]:
                        rows.append({'text': ''.join(ln.split()), 'box': (60, y, 420, 20)})
                        y += 30
                    return img, rows
                if inner_self.view == 'detail' and inner_self.posts:
                    post = inner_self.posts[0]
                    rows.append({'text': ''.join(post['text'].splitlines()[0].split()), 'box': (60, 80, 420, 20)})
                    if post['own']:
                        img[190:206, 320:335] = test_self.own_delete
                    if post['visibility'] == 'private':
                        rows.append({'text': '私密', 'box': (60, 190, 40, 16)})
                    rows.append({'text': post['time'], 'box': (60, 190, 60, 16)})
                    img[240, 24:-24] = 232
                    return img, rows
                y = 80
                for post in inner_self.posts:
                    if y >= h - 20:
                        break
                    img[y:y + 4, 24:40] = sum(ord(c) for c in post.get('tag', '')) % 200
                    for ln in [ln for ln in post['text'].splitlines() if ln.strip()]:
                        rows.append({'text': ''.join(ln.split()), 'box': (60, y + 8, 420, 18)})
                        y += 28
                    footer_y = y + 45
                    if footer_y + 30 > h:
                        break
                    if post['own']:
                        img[footer_y:footer_y + 16, 320:335] = test_self.own_delete
                    rows.append({'text': post.get('time', '刚刚'), 'box': (60, footer_y, 70, 16)})
                    sep_y = footer_y + 28
                    if sep_y < h:
                        img[sep_y, 24:-24] = 232
                    y = sep_y + 12
                return img, rows

        return FakeAdhocDesktop

    def _write_manual_reply_view(
        self,
        *,
        enabled=True,
        manual_reply_enabled=True,
        paused=False,
        max_age_seconds=3600,
        lease_expires_at=None,
        mode=0o640,
        account_id='own-id',
        target=None,
    ):
        p = Path(target) if target is not None else (self.root / 'config/octopus-manual-reply.json')
        p.write_text(json.dumps({
            'schema_version': 1,
            'account_sha256': worker.sha(account_id),
            'enabled': enabled,
            'manual_reply_enabled': manual_reply_enabled,
            'max_age_seconds': max_age_seconds,
            'paused': paused,
            'lease_expires_at': lease_expires_at,
        }), encoding='utf-8')
        p.chmod(mode)
        return p

    def _apply_notice_origin(
        self,
        job,
        *,
        job_id,
        now,
        origin_notice_id=None,
        origin_notice_key=None,
        origin_notice_observed_at=None,
        origin_notice_created_at=None,
    ):
        if origin_notice_id is None and origin_notice_key is None:
            return
        key = origin_notice_key or f'inbound:batch-{job_id[:14]}'
        nid = worker.format_notice_id(key) if origin_notice_id is True else origin_notice_id
        if nid is not None:
            job['origin_notice_id'] = nid
        job['origin_notice_key'] = key
        job['origin_notice_observed_at'] = now - 5000 if origin_notice_observed_at is None else origin_notice_observed_at
        job['origin_notice_created_at'] = now - 1000 if origin_notice_created_at is None else origin_notice_created_at

    def _group_job(
        self,
        job_id,
        contact_index=1,
        kind='send',
        text='群聊实时播报',
        origin_notice_id=None,
        origin_notice_key=None,
        origin_notice_observed_at=None,
        origin_notice_created_at=None,
        created_at=None,
        expires_at=None,
    ):
        contact = self.sidecar['contacts'][contact_index]
        now = int(time.time() * 1000) if created_at is None else created_at
        body = '' if kind == 'probe' else text
        job = {
            'schema_version': 2, 'channel': 'group', 'kind': kind, 'job_id': job_id,
            'account_sha256': worker.sha('own-id'),
            'contact_id': contact['id'], 'contact_type': 'group',
            'group_id': contact['group_id'], 'group_name': contact['group_name'],
            'group_alias': contact['alias'],
            'binding_sha256': worker.compute_group_binding_sha256(self.root, self.config, contact),
            'text': body, 'text_sha256': worker.sha(body),
            'created_at': now, 'expires_at': now + 600000 if expires_at is None else expires_at,
            'confirmation_request_id': f'req-{job_id[:6]}',
        }
        self._apply_notice_origin(
            job, job_id=job_id, now=now, origin_notice_id=origin_notice_id,
            origin_notice_key=origin_notice_key, origin_notice_observed_at=origin_notice_observed_at,
            origin_notice_created_at=origin_notice_created_at,
        )
        p = self.root / 'adhoc/outbox' / f'{job_id}.json'
        p.write_text(json.dumps(job), encoding='utf-8')
        p.chmod(0o640)
        return p

    def _friend_job(
        self,
        job_id,
        text='好友手动回复测试',
        origin_notice_id=None,
        origin_notice_key=None,
        origin_notice_observed_at=None,
        origin_notice_created_at=None,
        created_at=None,
        expires_at=None,
    ):
        contact = self.sidecar['contacts'][0]
        now = int(time.time() * 1000) if created_at is None else created_at
        job = {
            'schema_version': 2, 'kind': 'send', 'job_id': job_id,
            'account_sha256': worker.sha('own-id'),
            'contact_id': contact['id'], 'contact_type': 'friend',
            'recipient_id': contact['recipient_id'], 'recipient_alias': contact['alias'],
            'binding_sha256': worker.compute_binding_sha256(self.root, self.config, contact),
            'text': text, 'text_sha256': worker.sha(text),
            'created_at': now, 'expires_at': now + 600000 if expires_at is None else expires_at,
            'confirmation_request_id': f'req-{job_id[:6]}',
        }
        self._apply_notice_origin(
            job, job_id=job_id, now=now, origin_notice_id=origin_notice_id,
            origin_notice_key=origin_notice_key, origin_notice_observed_at=origin_notice_observed_at,
            origin_notice_created_at=origin_notice_created_at,
        )
        p = self.root / 'adhoc/outbox' / f'{job_id}.json'
        p.write_text(json.dumps(job), encoding='utf-8')
        p.chmod(0o640)
        return p

    def _adhoc_job(self, job_id, text='按需朋友圈正文', visibility='private', created_ms=None):
        now = int(time.time() * 1000) if created_ms is None else created_ms
        trade_date = datetime.fromtimestamp(now / 1000, ZoneInfo('Asia/Shanghai')).date().isoformat()
        binding = worker.compute_adhoc_moments_binding_sha256(self.root, self.config, visibility, worker.sha(text))
        job = {
            'schema_version': 2, 'channel': 'adhoc_moments', 'kind': 'publish', 'job_id': job_id,
            'account_sha256': worker.sha('own-id'), 'visibility': visibility,
            'binding_sha256': binding, 'text': text, 'text_sha256': worker.sha(text),
            'trade_date': trade_date, 'created_at': now, 'expires_at': now + 600000,
            'confirmation_request_id': f'req-{job_id[:6]}',
        }
        p = self.root / 'adhoc/outbox' / f'{job_id}.json'
        p.write_text(json.dumps(job), encoding='utf-8')
        p.chmod(0o640)
        return p

    def test_real_group_desktop_send_probe_and_isolation(self):
        fake_cls = self._make_group_desktop_cls()
        p_send = self._group_job('g1' + '0' * 22 if False else '1a' * 12, contact_index=1, kind='send', text='策略群盘后同步')
        res = worker.process_job(
            p_send, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), friend_label_anchor=self.root / 'config/octopus-friend-label.png',
            config_path=self.config_path, contacts_path=self.contacts_path, group_desktop_class=fake_cls,
        )
        self.assertEqual(res['state'], 'sent_verified')
        self.assertEqual(res['proof']['group_id'], 'wx-group-ops')
        self.assertEqual(res['proof']['job_id'], '1a' * 12)
        ev_file = self.root / 'receipts/evidence' / res['proof']['evidence_file']
        self.assertTrue(ev_file.is_file())

        # Read-only probe on group with group_enabled=False works without typing/submitting.
        p_probe = self._group_job('2b' * 12, contact_index=2, kind='probe')
        submits_before = fake_cls.submits
        probe_res = worker.process_job(
            p_probe, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            group_desktop_class=fake_cls,
        )
        self.assertEqual(probe_res['state'], 'identity_verified')
        self.assertEqual(probe_res['group_id'], 'wx-group-bak')
        self.assertEqual(fake_cls.submits, submits_before)

        # Friend sidebar on UI rejects cleanly (compatible_desktop never injects '朋友资料' onto groups).
        friend_sidebar_cls = self._make_group_desktop_cls({'friend_sidebar': True})
        p_friend_ui = self._group_job('3c' * 12, contact_index=1, kind='send', text='误进私聊检查')
        bad_ui_res = worker.process_job(
            p_friend_ui, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            group_desktop_class=friend_sidebar_cls,
        )
        self.assertEqual(bad_ui_res['state'], 'failed_before_submit')
        self.assertEqual(bad_ui_res['reason'], 'direct_conversation_rejected')
        self.assertEqual(friend_sidebar_cls.submits, 0)

        # Tampering member anchor blocks BEFORE submit.
        class PreSubmitAnchorTamperDesktop(fake_cls):
            def prepare(inner_self, text, group_id, *, trade_date=None, job_id=None):
                out = super().prepare(text, group_id, trade_date=trade_date, job_id=job_id)
                (test_self_root / 'config/groups/ops-group/member-2.png').write_bytes(b'tampered-after-prepare')
                return out

        test_self_root = self.root
        p_tamper = self._group_job('4d' * 12, contact_index=1, kind='send', text='防篡改检查')
        tamper_res = worker.process_job(
            p_tamper, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            group_desktop_class=PreSubmitAnchorTamperDesktop,
        )
        self.assertEqual(tamper_res['state'], 'failed_before_submit')
        self.assertEqual(tamper_res['reason'], 'binding_digest_mismatch')

    def test_real_adhoc_moments_desktop_limits_visibility_and_independent_evidence(self):
        feed = [{'text': '历史动态：保留基线参考。', 'own': True, 'visibility': 'friends', 'time': '2小时前', 'tag': 'old-1'}]
        adhoc_cls = self._make_adhoc_desktop_cls(feed)

        # Disabled adhoc_moments refuses even if daily moments_enabled is True.
        cfg_daily_only = dict(self.config, moments_enabled=True, adhoc_moments_enabled=False)
        self.config_path.write_text(json.dumps(cfg_daily_only), encoding='utf-8')
        p_dis = self._adhoc_job('5a' * 12, text='独立开关检查', visibility='friends')
        dis_res = worker.process_job(
            p_dis, self.root, cfg_daily_only, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            adhoc_desktop_class=adhoc_cls,
        )
        self.assertEqual(dis_res['state'], 'refused')
        self.assertEqual(dis_res['reason'], 'adhoc_moments_disabled')
        self.config_path.write_text(json.dumps(self.config), encoding='utf-8')

        # Oversized (>220 non-ws chars) fails without altering original text.
        too_long = '中' * 221
        p_bad = self._adhoc_job('5b' * 12, text=too_long, visibility='private')
        bad_res = worker.process_job(
            p_bad, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            adhoc_desktop_class=adhoc_cls,
        )
        self.assertEqual(bad_res['state'], 'refused')
        self.assertEqual(json.loads(p_bad.read_text(encoding='utf-8'))['text'], too_long)

        # Two separate 24-hex requests with identical text publish independently with distinct evidence files.
        same_text = '同一文案两次独立发送'
        p1 = self._adhoc_job('61' * 12, text=same_text, visibility='friends')
        r1 = worker.process_job(
            p1, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            adhoc_desktop_class=adhoc_cls,
        )
        self.assertEqual(r1['state'], 'published_verified')
        self.assertEqual(r1['proof']['job_id'], '61' * 12)
        ev1 = self.root / 'adhoc/receipts/evidence' / r1['proof']['evidence_file']
        self.assertEqual(ev1.name, f"{'61' * 12}.{worker.sha(same_text)[:12]}.png")
        self.assertTrue(ev1.is_file())

        p2 = self._adhoc_job('62' * 12, text=same_text, visibility='private')
        r2 = worker.process_job(
            p2, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            adhoc_desktop_class=adhoc_cls,
        )
        self.assertEqual(r2['state'], 'published_verified')
        self.assertEqual(r2['proof']['job_id'], '62' * 12)
        ev2 = self.root / 'adhoc/receipts/evidence' / r2['proof']['evidence_file']
        self.assertNotEqual(ev1, ev2)
        self.assertTrue(ev2.is_file())
        self.assertEqual(adhoc_cls.submits, 2)

    def test_adhoc_account_lock_daily_coexistence_and_no_double_lock(self):
        feed = [{'text': '基线好友动态', 'own': False, 'visibility': 'friends', 'time': '1小时前', 'tag': 'f-1'}]
        adhoc_cls = self._make_adhoc_desktop_cls(feed)
        group_cls = self._make_group_desktop_cls()
        daily_q = Queue(self.root / 'outbox', self.root / 'receipts')

        p_adhoc = self._adhoc_job('71' * 12, text='锁竞争测试朋友圈', visibility='private')
        p_group = self._group_job('72' * 12, contact_index=1, kind='send', text='锁竞争测试群')
        p_probe = self._group_job('73' * 12, contact_index=2, kind='probe')

        with daily_q.lock(worker.sha('own-id')):
            self.assertEqual(worker.process_job(
                p_adhoc, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                adhoc_desktop_class=adhoc_cls,
            )['state'], 'busy')
            self.assertEqual(worker.process_job(
                p_group, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                group_desktop_class=group_cls,
            )['state'], 'busy')
            self.assertEqual(worker.process_job(
                p_probe, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                group_desktop_class=group_cls,
            )['state'], 'busy')

        # Once the shared account lock clears, both run cleanly without re-locking deadlock.
        self.assertEqual(worker.process_job(
            p_adhoc, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            adhoc_desktop_class=adhoc_cls,
        )['state'], 'published_verified')
        self.assertEqual(worker.process_job(
            p_group, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            group_desktop_class=group_cls,
        )['state'], 'sent_verified')

        # Daily 64-hex job creation and dedup remain untouched by adhoc jobs.
        from gamma_common.report_versions import CN_REPORT_TEAM_VERSION
        runs_dir = self.root / 'runs'
        runs_dir.mkdir(exist_ok=True)
        daily_rid = 'a' * 32
        (runs_dir / f'{daily_rid}.json').write_text(json.dumps({
            'version': CN_REPORT_TEAM_VERSION, 'run_id': daily_rid, 'hash': 'h' * 64,
            'report_as_of': '2026-11-21T15:00:00+08:00',
            'input': {'evidence': {
                'E1': {'symbol': '510500', 'instrument_scope': 'etf', 'category': 'daily_price',
                       'source_date': '2026-11-21', 'close': 6.2, 'adjustment': 'raw',
                       'price_basis': 'as_published_session_close'},
                'E2': {'symbol': '510500', 'instrument_scope': 'etf', 'category': 'latest_price',
                       'source_date': '2026-11-21', 'reference_date': '2026-11-20',
                       'reference_close': 6.4, 'adjustment': 'raw'},
            }},
            'claim_states': {'O': 'disputed', 'I1': 'accepted', 'I2': 'withdrawn'},
            'analysis': {
                'outlook': {'direction': '弱势震荡，反弹持续性待确认', 'confidence': '低', 'horizon_sessions': 3, 'fact_refs': ['E1.close']},
                'insights': [
                    {'topic': '持仓', 'hypothesis': '增仓下跌偏向空头压力，移仓仍可能',
                     'alternative': '移仓和套保也可解释增仓', 'confirm': '价格继续承压且持仓增加',
                     'invalidate': '价格收复阻力且持仓回落', 'fact_refs': ['E1.close']},
                ],
            },
            'output': (
                '## Gamma解读（AI研究团队）\n\n综合判断〔有争议〕：\n\n'
                '- 弱势震荡，反弹持续性待确认。未来3个交易日；定性置信度低。\n'
                '\n**持仓〔双审通过〕**\n\n- 增仓下跌偏向空头压力，移仓仍可能。\n'
                '- 竞争解释：移仓和套保也可解释增仓。\n'
                '\n**持仓**\n\n- 支持条件：价格继续承压且持仓增加。\n'
                '- 否证条件：价格收复阻力且持仓回落。\n'
            ),
        }), encoding='utf-8')
        journal = self.root / 'attempts.jsonl'
        journal.write_text(json.dumps({
            'attempt_id': 'a1', 'mode': 'daily_close', 'trade_date': '2026-11-21',
            'finished_at': '2026-11-21T15:05:00+08:00', 'sent': True, 'run_id': daily_rid,
            'team_section_retained': True, 'archive_status': 'written',
            'research_input_hash': 'h' * 64, 'final_body_sha256': 'f' * 64,
        }) + '\n', encoding='utf-8')
        now_dt = datetime(2026, 11, 21, 15, 10, tzinfo=ZoneInfo('Asia/Shanghai'))
        d1 = produce(journal, runs_dir, daily_q, now=now_dt, account='own-id', visibility='private')
        d2 = produce(journal, runs_dir, daily_q, now=now_dt, account='own-id', visibility='private')
        self.assertEqual(d1['state'], 'queued')
        self.assertEqual(len(d1['job_id']), 64)
        self.assertEqual(d2['state'], 'already_queued')

    def test_presubmit_recheck_and_uncertain_reconcile_without_resubmit(self):
        feed = [{'text': '原有顶部动态', 'own': True, 'visibility': 'private', 'time': '1小时前', 'tag': 'p-old'}]
        adhoc_cls = self._make_adhoc_desktop_cls(feed, scenario={'post_then_raise': True})
        p = self._adhoc_job('81' * 12, text='崩溃后核对测试', visibility='private')

        res = worker.process_job(
            p, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            adhoc_desktop_class=adhoc_cls,
        )
        self.assertEqual(res['state'], 'needs_review')
        self.assertIn('preflight_proof', res)
        self.assertEqual(adhoc_cls.submits, 1)

        # Polling/re-running process_job NEVER resubmits or re-types!
        res_again = worker.process_job(
            p, self.root, self.config, Queue, run_one, Desktop,
            trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            adhoc_desktop_class=adhoc_cls,
        )
        self.assertEqual(res_again['state'], 'needs_review')
        self.assertEqual(adhoc_cls.submits, 1)

        # Explicit operator reconcile recovers proof without submitting again.
        adhoc_q = AdhocMomentsQueue(self.root / 'adhoc/outbox', self.root / 'adhoc/receipts', lock_dir=self.root / 'receipts')
        reconcile_cls = self._make_adhoc_desktop_cls(feed, scenario={})
        reconciled = reconcile(adhoc_q, '81' * 12, reconcile_cls(dict(self.config, adhoc_job_id='81' * 12)))
        self.assertEqual(reconciled['state'], 'published_verified')
        self.assertEqual(reconcile_cls.submits, 0)
        self.assertTrue((self.root / 'adhoc/receipts/evidence' / reconciled['proof']['evidence_file']).is_file())

    def test_end_to_end_installer_node_broker_and_python_worker_phase2(self):
        import shutil
        import subprocess
        from types import SimpleNamespace

        deploy_root = self.root / 'e2e'
        wechat_dir = deploy_root / 'wechat'
        source_dir = deploy_root / 'src'
        release_root = deploy_root / 'releases'
        backup_root = deploy_root / 'backups'
        state_dir = deploy_root / 'state'
        policy_path = deploy_root / 'etc/octopus/operations.json'
        sock_path = self.root / 'e2e.sock'
        ingress_db = deploy_root / 'ingress.sqlite'
        ingress_profile = deploy_root / 'ingress/cordis.patch.yml'
        agent_profile = deploy_root / 'agent/profile'
        unit_dir = deploy_root / 'systemd'
        for d in [wechat_dir / 'config/groups/ops-group', wechat_dir / 'config/groups/bak-group',
                  wechat_dir / 'receipts/evidence', wechat_dir / 'adhoc/receipts/evidence',
                  source_dir, policy_path.parent, ingress_profile.parent, agent_profile, unit_dir]:
            d.mkdir(parents=True, exist_ok=True)
        for fn in installer.RELEASE_FILES:
            shutil.copyfile(Path(__file__).parents[1] / fn, source_dir / fn)
        for rel in ['config/account.png', 'config/own-chat.png', 'config/octopus-friend-label.png',
                    'config/recipient.png', 'config/recipient-sidebar.png', 'config/recipient-card.png',
                    'config/groups/ops-group/group-search.png', 'config/groups/ops-group/member-1.png',
                    'config/groups/ops-group/member-2.png', 'config/groups/ops-group/group-name.png',
                    'config/groups/bak-group/group-search.png', 'config/groups/bak-group/member-1.png']:
            shutil.copyfile(self.root / rel, wechat_dir / rel)
        (wechat_dir / 'config/config.json').write_text(json.dumps(self.config), encoding='utf-8')
        (wechat_dir / 'receipts/worker.json').write_text(json.dumps({'checked_at': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(), 'state': 'ready'}))

        binding_key = json.dumps(['feishu', 'app', 'org', 'chat', 'owner'], separators=(',', ':'))
        with closing(sqlite3.connect(ingress_db)) as conn:
            conn.execute('CREATE TABLE u_im_ingress_operators(key TEXT PRIMARY KEY, value TEXT)')
            conn.execute('CREATE TABLE u_im_ingress_deliveries(key TEXT PRIMARY KEY, value TEXT)')
            conn.execute('INSERT INTO u_im_ingress_operators VALUES(?,?)',
                         (binding_key, json.dumps({'operatorId': 'owner', 'targetTenantId': 'octopus', 'revokedAt': None})))
            conn.commit()
        ingress_profile.write_text('operatorBindings: ' + json.dumps([{'bindingKey': binding_key, 'operatorId': 'owner', 'targetTenantId': 'octopus'}]) + '\n')
        (agent_profile / 'cordis.patch.yml').write_text(json.dumps([{'id': 'im-runtime', 'inject': ['agents']}]))
        (agent_profile / 'package.json').write_text(json.dumps({'name': 'p', 'dependencies': {}}))

        contacts_file = deploy_root / 'contacts.json'
        contacts_file.write_text(json.dumps(self.sidecar['contacts']), encoding='utf-8')
        adhoc_file = deploy_root / 'adhoc.json'
        adhoc_file.write_text(json.dumps({'enabled': True, 'defaultVisibility': 'private'}), encoding='utf-8')

        fake_cmd = SimpleNamespace(DEVNULL=0, call=lambda *a, **kw: 0, check_call=lambda *a, **kw: 0, check_output=lambda *a, **kw: b'0\n')
        srv = __import__('socket').socket(__import__('socket').AF_UNIX, __import__('socket').SOCK_STREAM)
        srv.bind(str(sock_path))
        srv.listen(4)
        self.addCleanup(srv.close)

        node_bin = shutil.which('node') or '/usr/bin/node'
        summary = installer.install(
            source_dir, deploy_root / 'runtime', Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=sock_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, contacts_config=contacts_file, adhoc_moments_config=adhoc_file,
            check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
        )
        self.assertEqual(summary['adhocMoments'], {'enabled': True, 'defaultVisibility': 'private'})
        self.assertEqual(len(summary['contacts']), 3)

        def run_broker(action_script):
            env = {**os.environ, 'POLICY_PATH': str(policy_path), 'RELEASE_CUR': str(release_root / 'current'),
                   'INGRESS_DB': str(ingress_db), 'BINDING_KEY': binding_key}
            res = subprocess.run([node_bin, '--input-type=module', '-e', action_script], capture_output=True, text=True, env=env, check=True)
            return json.loads(res.stdout)

        ops = run_broker("""
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { join } from 'node:path';
const cur = process.env.RELEASE_CUR;
const { validatePolicy } = await import(pathToFileURL(join(cur, 'policy.mjs')).href);
const { Operations } = await import(pathToFileURL(join(cur, 'operations.mjs')).href);
const { dispatch, perform } = await import(pathToFileURL(join(cur, 'broker.mjs')).href);
const db = new DatabaseSync(process.env.INGRESS_DB);
function human(id, prompt) {
  db.prepare('DELETE FROM u_im_ingress_deliveries').run();
  db.prepare('INSERT INTO u_im_ingress_deliveries VALUES(?,?)').run('c', JSON.stringify({
    status: 'in_flight', operatorId: 'owner', targetTenantId: 'octopus', requestId: id,
    prompt, receivedAt: new Date().toISOString(),
    deliveryKey: JSON.stringify(['feishu','app','org','chat','owner',id]),
  }));
}
const policy = validatePolicy(JSON.parse(readFileSync(process.env.POLICY_PATH, 'utf8')));
const j = new Operations(policy, process.env.POLICY_PATH, perform);
human('h-probe', '核验只读群');
const probeOp = j.prepare({ kind: 'wechat_probe_group', payload: { recipient: '只读核验群' } });
await dispatch({ action: 'execute', params: { id: probeOp.id } }, j);
human('h-group', '给策略投研群发群消息：全链路群消息验证');
const grpOp = j.prepare({ kind: 'wechat_send_group', payload: { recipient: '策略投研群', text: '全链路群消息验证' } });
await dispatch({ action: 'execute', params: { id: grpOp.id } }, j);
human('h-mom', '发朋友圈：全链路按需朋友圈验证');
const momOp = j.prepare({ kind: 'wechat_publish_moment', payload: { text: '全链路按需朋友圈验证' } });
await dispatch({ action: 'execute', params: { id: momOp.id } }, j);
j.close();
db.close();
console.log(JSON.stringify({ probeId: probeOp.id, grpId: grpOp.id, momId: momOp.id }));
""")
        installed_cfg = json.loads((wechat_dir / 'config/config.json').read_text(encoding='utf-8'))
        installed_sidecar = wechat_dir / 'config/octopus-contacts.json'
        group_cls = self._make_group_desktop_cls()
        adhoc_cls = self._make_adhoc_desktop_cls([])

        for jid, g_cls, a_cls in [
            (ops['probeId'], group_cls, None),
            (ops['grpId'], group_cls, None),
            (ops['momId'], None, adhoc_cls),
        ]:
            worker.process_job(
                wechat_dir / 'adhoc/outbox' / f"{jid}.json", wechat_dir, installed_cfg,
                Queue, run_one, Desktop, trusted_uid=os.getuid(),
                config_path=wechat_dir / 'config/config.json', contacts_path=installed_sidecar,
                group_desktop_class=g_cls, adhoc_desktop_class=a_cls,
            )

        receipts = run_broker(f"""
import {{ readFileSync }} from 'node:fs';
import {{ pathToFileURL }} from 'node:url';
import {{ join }} from 'node:path';
const cur = process.env.RELEASE_CUR;
const {{ validatePolicy }} = await import(pathToFileURL(join(cur, 'policy.mjs')).href);
const {{ Operations }} = await import(pathToFileURL(join(cur, 'operations.mjs')).href);
const {{ dispatch, perform }} = await import(pathToFileURL(join(cur, 'broker.mjs')).href);
const policy = validatePolicy(JSON.parse(readFileSync(process.env.POLICY_PATH, 'utf8')));
const j = new Operations(policy, process.env.POLICY_PATH, perform);
const rProbe = await dispatch({{ action: 'receipt', params: {{ id: {json.dumps(ops['probeId'])} }} }}, j);
const rGrp = await dispatch({{ action: 'receipt', params: {{ id: {json.dumps(ops['grpId'])} }} }}, j);
const rMom = await dispatch({{ action: 'receipt', params: {{ id: {json.dumps(ops['momId'])} }} }}, j);
j.close();
console.log(JSON.stringify({{ rProbe, rGrp, rMom }}));
""")
        self.assertEqual(receipts['rProbe']['state'], 'completed')
        self.assertEqual(receipts['rProbe']['workerReceipt']['state'], 'identity_verified')
        self.assertEqual(receipts['rGrp']['state'], 'completed')
        self.assertEqual(receipts['rGrp']['workerReceipt']['state'], 'sent_verified')
        self.assertEqual(receipts['rMom']['state'], 'completed')
        self.assertEqual(receipts['rMom']['workerReceipt']['state'], 'published_verified')
        self.assertEqual(receipts['rMom']['visibility'], 'private')

    def test_manual_reply_worker_validation_lock_tamper_and_no_replay(self):
        Desktop.submissions = []
        Desktop.fail_after_submit = False
        manual_view = self.root / 'config/octopus-manual-reply.json'
        try:
            # 0. Default-off: missing trusted octopus-manual-reply.json refuses notice replies, while normal non-reply sends STILL succeed!
            p_missing_cfg = self._friend_job('90' * 12, text='未启用时禁止通知回复', origin_notice_id=True)
            self.assertEqual(
                worker.process_job(
                    p_missing_cfg, self.root, self.config, Queue, run_one, Desktop,
                    trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                )['reason'],
                'manual_reply_disabled',
            )
            p_normal_send = self._friend_job('91' * 12, text='普通主动发送不受通知开关影响')
            self.assertEqual(
                worker.process_job(
                    p_normal_send, self.root, self.config, Queue, run_one, Desktop,
                    trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                )['state'],
                'sent_verified',
            )
            Desktop.submissions = []

            # World-writable trusted view is refused cleanly.
            self._write_manual_reply_view(mode=0o666)
            p_bad_mode = self._friend_job('92' * 12, text='权限不安全视图', origin_notice_id=True)
            self.assertEqual(
                worker.process_job(
                    p_bad_mode, self.root, self.config, Queue, run_one, Desktop,
                    trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                )['reason'],
                'untrusted_manual_reply_config',
            )
            self._write_manual_reply_view(mode=0o640)

            # 1. Valid friend & group replies with matching origin_notice_* fields & trusted view preserve lock safety.
            p_friend = self._friend_job('9a' * 12, text='好友定向回复正文', origin_notice_id=True)
            q = Queue(self.root / 'outbox', self.root / 'receipts')
            with q.lock(worker.sha('own-id')):
                busy_res = worker.process_job(
                    p_friend, self.root, self.config, Queue, run_one, Desktop,
                    trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                )
            self.assertEqual(busy_res['state'], 'busy')
            self.assertEqual(Desktop.submissions, [])

            friend_res = worker.process_job(
                p_friend, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            )
            self.assertEqual(friend_res['state'], 'sent_verified')
            self.assertEqual(Desktop.submissions, ['好友定向回复正文'])

            group_cls = self._make_group_desktop_cls()
            p_group = self._group_job('9b' * 12, contact_index=1, kind='send', text='群通知定向回复正文', origin_notice_id=True)
            group_res = worker.process_job(
                p_group, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                group_desktop_class=group_cls,
            )
            self.assertEqual(group_res['state'], 'sent_verified')

            # 2. Malformed/mismatched/partial origin fields or origin_notice_* on probe/adhoc_moments are refused without UI launch.
            for idx, bad_id in enumerate(('bad-notice', 'wn_UPPERCASE123', 'wn_12345')):
                p_bad = self._friend_job(f"ba{idx:022x}", text='非法通知编号', origin_notice_id=bad_id)
                bad_res = worker.process_job(
                    p_bad, self.root, self.config, Queue, run_one, Desktop,
                    trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                )
                self.assertEqual(bad_res['state'], 'refused')
                self.assertEqual(bad_res['reason'], 'invalid_origin_notice_id')

            p_mismatch = self._friend_job('bb' * 12, text='编号与key不一致', origin_notice_id='wn_000000000000', origin_notice_key='inbound:batch-real')
            self.assertEqual(
                worker.process_job(
                    p_mismatch, self.root, self.config, Queue, run_one, Desktop,
                    trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                )['reason'],
                'origin_notice_identity_mismatch',
            )

            p_partial = self._friend_job('bc' * 12, text='缺失完整授权字段')
            partial_obj = json.loads(p_partial.read_text(encoding='utf-8'))
            partial_obj['origin_notice_id'] = 'wn_0123456789ab'
            p_partial.write_text(json.dumps(partial_obj), encoding='utf-8')
            p_partial.chmod(0o640)
            self.assertEqual(
                worker.process_job(
                    p_partial, self.root, self.config, Queue, run_one, Desktop,
                    trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                )['reason'],
                'invalid_job_fields',
            )

            p_probe_bad = self._group_job('9c' * 12, contact_index=2, kind='probe', origin_notice_id=True)
            probe_bad_res = worker.process_job(
                p_probe_bad, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                group_desktop_class=group_cls,
            )
            self.assertEqual(probe_bad_res['state'], 'refused')
            self.assertEqual(probe_bad_res['reason'], 'invalid_job_fields')

            p_mom_bad = self._adhoc_job('9d' * 12, text='朋友圈禁止携带通知来源')
            mom_obj = json.loads(p_mom_bad.read_text(encoding='utf-8'))
            mom_obj['origin_notice_id'] = 'wn_0123456789ab'
            p_mom_bad.write_text(json.dumps(mom_obj), encoding='utf-8')
            p_mom_bad.chmod(0o640)
            mom_bad_res = worker.process_job(
                p_mom_bad, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                adhoc_desktop_class=self._make_adhoc_desktop_cls([]),
            )
            self.assertEqual(mom_bad_res['state'], 'refused')
            self.assertEqual(mom_bad_res['reason'], 'invalid_job_fields')

            # 3. Pre-submit recheck detects tampered frozen fields (origin/expires/confirmation) BEFORE submit().
            p_tamper = self._group_job('9e' * 12, contact_index=1, kind='send', text='二次核验全字段篡改', origin_notice_id=True)

            class OriginTamperDesktop(group_cls):
                def prepare(inner_self, text, group_id, *, trade_date=None, job_id=None):
                    out = super().prepare(text, group_id, trade_date=trade_date, job_id=job_id)
                    raw = json.loads(p_tamper.read_text(encoding='utf-8'))
                    raw['origin_notice_key'] = 'inbound:tampered-key'
                    raw['origin_notice_id'] = worker.format_notice_id(raw['origin_notice_key'])
                    raw['expires_at'] = raw['expires_at'] + 1000
                    p_tamper.write_text(json.dumps(raw), encoding='utf-8')
                    p_tamper.chmod(0o640)
                    return out

            submits_before = OriginTamperDesktop.submits
            tamper_res = worker.process_job(
                p_tamper, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
                group_desktop_class=OriginTamperDesktop,
            )
            self.assertEqual(tamper_res['state'], 'failed_before_submit')
            self.assertEqual(tamper_res['reason'], 'binding_changed_before_submit')
            self.assertEqual(OriginTamperDesktop.submits, submits_before)

            # 4. Crash after submit produces needs_review and NEVER replays on retry.
            p_crash = self._friend_job('9f' * 12, text='崩溃后禁止重发', origin_notice_id=True)
            Desktop.fail_after_submit = True
            Desktop.submissions = []
            crash_res = worker.process_job(
                p_crash, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            )
            self.assertEqual(crash_res['state'], 'needs_review')
            self.assertEqual(Desktop.submissions, ['崩溃后禁止重发'])
            Desktop.fail_after_submit = False
            replay_res = worker.process_job(
                p_crash, self.root, self.config, Queue, run_one, Desktop,
                trusted_uid=os.getuid(), config_path=self.config_path, contacts_path=self.contacts_path,
            )
            self.assertEqual(replay_res['state'], 'needs_review')
            self.assertEqual(Desktop.submissions, ['崩溃后禁止重发'])
        finally:
            manual_view.unlink(missing_ok=True)
            Desktop.submissions = []
            Desktop.fail_after_submit = False

    def test_manual_reply_short_deadline_live_pause_and_frozen_authority(self):
        import hashlib
        import shutil
        import subprocess

        # Integrated Node -> Python short-boundary test: notice with maxAge=60s observed 59s ago (1s remaining TTL).
        work = (self.root / 'short_boundary').resolve()
        wechat_dir = work / 'wechat'
        state_dir = work / 'state'
        handoff_dir = work / 'handoff'
        policy_path = work / 'policy.json'
        ingress_db = work / 'ingress.sqlite'
        wechat_cfg_path = wechat_dir / 'config/config.json'
        sidecar_path = wechat_dir / 'config/octopus-contacts.json'
        for p, mode in [
            (work, 0o750), (wechat_dir, 0o750), (wechat_dir / 'config', 0o750),
            (wechat_dir / 'config/groups/ops-group', 0o750), (wechat_dir / 'config/groups/bak-group', 0o750),
            (wechat_dir / 'adhoc', 0o750), (wechat_dir / 'adhoc/outbox', 0o750),
            (wechat_dir / 'adhoc/receipts', 0o700), (wechat_dir / 'adhoc/receipts/evidence', 0o700),
            (wechat_dir / 'receipts', 0o750), (wechat_dir / 'receipts/evidence', 0o750),
            (wechat_dir / 'inbox', 0o700), (wechat_dir / 'inbox/events', 0o700), (wechat_dir / 'inbox/evidence', 0o700),
            (state_dir, 0o700), (handoff_dir, 0o750),
        ]:
            p.mkdir(parents=True, exist_ok=True)
            p.chmod(mode)
        for rel in ['config/account.png', 'config/own-chat.png', 'config/octopus-friend-label.png',
                    'config/recipient.png', 'config/recipient-sidebar.png', 'config/recipient-card.png',
                    'config/groups/ops-group/group-search.png', 'config/groups/ops-group/member-1.png',
                    'config/groups/ops-group/member-2.png', 'config/groups/ops-group/group-name.png',
                    'config/groups/bak-group/group-search.png', 'config/groups/bak-group/member-1.png']:
            shutil.copyfile(self.root / rel, wechat_dir / rel)
        wechat_cfg_path.write_text(json.dumps(self.config), encoding='utf-8')
        sidecar_path.write_text(json.dumps(self.sidecar), encoding='utf-8')
        sidecar_path.chmod(0o640)

        binding_key = json.dumps(['feishu', 'app', 'org', 'chat', 'owner'], separators=(',', ':'))
        with closing(sqlite3.connect(ingress_db)) as conn:
            conn.execute('CREATE TABLE u_im_ingress_operators(key TEXT PRIMARY KEY, value TEXT)')
            conn.execute('CREATE TABLE u_im_ingress_deliveries(key TEXT PRIMARY KEY, value TEXT)')
            conn.execute('INSERT INTO u_im_ingress_operators VALUES(?,?)',
                         (binding_key, json.dumps({'operatorId': 'owner', 'targetTenantId': 'octopus', 'revokedAt': None})))
            conn.commit()

        policy = {
            'socket': str(work / 'broker.sock'),
            'state': str(state_dir),
            'ingressDatabase': str(ingress_db),
            'bindingKey': binding_key,
            'operatorId': 'owner',
            'tenantId': 'octopus',
            'services': {'probe': 'octopus-probe.service'},
            'containers': ['probe'],
            'wechatRoot': str(wechat_dir),
            'wechatConfig': str(wechat_cfg_path),
            'wechatContactsConfig': str(sidecar_path),
            'wechatContacts': [
                {'id': 'friend-1', 'type': 'friend', 'alias': '好友一', 'displayName': '阿明', 'enabled': True, 'capabilities': {'directSend': True}},
                {'id': 'group-ops', 'type': 'group', 'alias': '策略投研群', 'displayName': '策略投研正式群', 'enabled': True, 'capabilities': {'groupSend': True}},
                {'id': 'group-bak', 'type': 'group', 'alias': '只读核验群', 'displayName': '备份观察群', 'enabled': True, 'capabilities': {'groupSend': False}},
            ],
            'wechatNotifications': {
                'enabled': True,
                'handoffDir': str(handoff_dir),
                'handoffGid': os.getgid(),
                'receiptProducerUid': os.getuid(),
                'inboundProducerUid': os.getuid(),
                'outboxProducerUid': os.getuid(),
                'manualReplyEnabled': True,
                'manualReplyMaxAgeSeconds': 60,
            },
            'limits': {'logLines': 50, 'previewSeconds': 600, 'workerPollSeconds': 2, 'maxTextBytes': 1000, 'commandSeconds': 10},
        }
        policy_path.write_text(json.dumps(policy), encoding='utf-8')
        policy_path.chmod(0o600)

        t0_ms = int(time.time() * 1000)
        obs_ms = t0_ms - 59_000  # observed 59s ago under 60s maxAge -> only 1s remaining at t0_ms!
        ev_bytes = b'short-ttl-worker-proof'
        (wechat_dir / 'inbox/evidence/short.png').write_bytes(ev_bytes)
        (wechat_dir / 'inbox/evidence/short.png').chmod(0o600)
        ops_group_proof = worker.compute_group_binding_sha256(wechat_dir, self.config, self.sidecar['contacts'][1])
        ev_path = wechat_dir / 'inbox/events/evt-short-grp-1.json'
        ev_path.write_text(json.dumps({
            'schema_version': 1,
            'event_id': 'evt-short-grp-1',
            'account_sha256': worker.sha('own-id'),
            'trigger_type': 'group_message',
            'conversation_kind': 'group',
            'conversation_ref': 'group-ops',
            'binding_sha256': ops_group_proof,
            'sender_name': '同事',
            'source_ref': 'msg-short-1',
            'source_time': '17:59',
            'observed_at': obs_ms,
            'text': '1秒限时群消息',
            'evidence_path': '/home/wechat/.gamma-moments/inbox/evidence/short.png',
            'evidence_sha256': hashlib.sha256(ev_bytes).hexdigest(),
        }), encoding='utf-8')
        ev_path.chmod(0o600)

        ops_dir = Path(__file__).parents[1]
        node_bin = shutil.which('node') or '/usr/bin/node'
        broker_out = json.loads(subprocess.check_output([
            node_bin, '--input-type=module', '-e',
            f"""
import {{ readFileSync }} from 'node:fs';
import {{ DatabaseSync }} from 'node:sqlite';
import {{ pathToFileURL }} from 'node:url';
const {{ validatePolicy }} = await import({json.dumps((ops_dir / 'policy.mjs').as_uri())});
const {{ Operations }} = await import({json.dumps((ops_dir / 'operations.mjs').as_uri())});
const {{ dispatch, perform, setManualReplyControl }} = await import({json.dumps((ops_dir / 'broker.mjs').as_uri())});
const {{ WechatExchangeProjector }} = await import({json.dumps((ops_dir / 'wechat-inbox.mjs').as_uri())});
const policy = validatePolicy(JSON.parse(readFileSync({json.dumps(str(policy_path))}, 'utf8')));
let clock = {t0_ms - 30_000};
setManualReplyControl(policy, {{ action: 'resume', nowMs: clock }});
const j = new Operations(policy, {json.dumps(str(policy_path))}, perform, () => clock);
const proj = new WechatExchangeProjector(policy, {{ clock: () => clock }});
const sealed = proj.projectOnce().sealed;
proj.close();
clock = {t0_ms};
const nid = sealed[0].noticeId;
const db = new DatabaseSync({json.dumps(str(ingress_db))});
function setPrompt(id, prompt) {{
  db.prepare('DELETE FROM u_im_ingress_deliveries').run();
  db.prepare('INSERT INTO u_im_ingress_deliveries VALUES(?,?)').run('c', JSON.stringify({{
    status: 'in_flight', operatorId: 'owner', targetTenantId: 'octopus', requestId: id,
    prompt, receivedAt: new Date(clock).toISOString(),
    deliveryKey: JSON.stringify(['feishu','app','org','chat','owner',id]),
  }}));
}}
setPrompt('req-s1', `回复 ${{nid}}：超时前拦截验证`);
const op1 = j.prepare({{ kind: 'wechat_reply_notice', payload: {{ noticeId: nid, text: '超时前拦截验证' }} }});
await dispatch({{ action: 'execute', params: {{ id: op1.id }} }}, j);
setPrompt('req-s2', `回复 ${{nid}}：暂停拦截验证`);
const op2 = j.prepare({{ kind: 'wechat_reply_notice', payload: {{ noticeId: nid, text: '暂停拦截验证' }} }});
await dispatch({{ action: 'execute', params: {{ id: op2.id }} }}, j);
j.close();
db.close();
console.log(JSON.stringify({{ nid, op1: op1.id, op2: op2.id }}));
""",
        ], text=True))

        outbox_1 = json.loads((wechat_dir / 'adhoc/outbox' / f"{broker_out['op1']}.json").read_text(encoding='utf-8'))
        self.assertEqual(outbox_1['expires_at'], obs_ms + 60_000)

        # Simulate clock crossing the 1s boundary right inside GroupDesktop.prepare() BEFORE preflight()/submit()!
        group_cls = self._make_group_desktop_cls()
        sim_clock = [t0_ms / 1000.0]

        class SlowPrepareGroupDesktop(group_cls):
            def prepare(inner_self, text, group_id, *, trade_date=None, job_id=None):
                out = super().prepare(text, group_id, trade_date=trade_date, job_id=job_id)
                sim_clock[0] = (obs_ms + 60_500) / 1000.0
                return out

        res_exp = worker.process_job(
            wechat_dir / 'adhoc/outbox' / f"{broker_out['op1']}.json", wechat_dir, self.config,
            Queue, run_one, Desktop, clock=lambda: sim_clock[0], trusted_uid=os.getuid(),
            config_path=wechat_cfg_path, contacts_path=sidecar_path,
            group_desktop_class=SlowPrepareGroupDesktop,
        )
        self.assertEqual(res_exp['state'], 'failed_before_submit')
        self.assertEqual(res_exp['reason'], 'expired_before_preflight')
        self.assertEqual(SlowPrepareGroupDesktop.submits, 0)
        # Re-running NEVER submits.
        self.assertEqual(
            worker.process_job(
                wechat_dir / 'adhoc/outbox' / f"{broker_out['op1']}.json", wechat_dir, self.config,
                Queue, run_one, Desktop, clock=lambda: obs_ms / 1000.0 + 10.0, trusted_uid=os.getuid(),
                config_path=wechat_cfg_path, contacts_path=sidecar_path,
                group_desktop_class=SlowPrepareGroupDesktop,
            )['state'],
            'failed_before_submit',
        )

        # Forged outbox job extending expires_at past min(observed, created)+max_age is independently refused by worker!
        forged_id = 'ab' * 12
        forged_path = wechat_dir / 'adhoc/outbox' / f'{forged_id}.json'
        forged_obj = {
            **outbox_1,
            'job_id': forged_id,
            'created_at': t0_ms,
            'expires_at': t0_ms + 600_000,
        }
        forged_path.write_text(json.dumps(forged_obj), encoding='utf-8')
        forged_path.chmod(0o640)
        forged_res = worker.process_job(
            forged_path, wechat_dir, self.config, Queue, run_one, Desktop,
            clock=lambda: (obs_ms + 60_500) / 1000.0, trusted_uid=os.getuid(),
            config_path=wechat_cfg_path, contacts_path=sidecar_path,
            group_desktop_class=group_cls,
        )
        self.assertEqual(forged_res['state'], 'refused')
        self.assertEqual(forged_res['reason'], 'manual_reply_notice_expired')

        # Pausing manual replies via setManualReplyControl blocks ALREADY-QUEUED task op2 without touching non-reply sends!
        subprocess.run([
            node_bin, '--input-type=module', '-e',
            f"""
import {{ readFileSync }} from 'node:fs';
const {{ setManualReplyControl }} = await import({json.dumps((ops_dir / 'broker.mjs').as_uri())});
const pol = JSON.parse(readFileSync({json.dumps(str(policy_path))}, 'utf8'));
setManualReplyControl(pol, {{ action: 'pause' }});
""",
        ], check=True)
        pause_res = worker.process_job(
            wechat_dir / 'adhoc/outbox' / f"{broker_out['op2']}.json", wechat_dir, self.config,
            Queue, run_one, Desktop, clock=lambda: t0_ms / 1000.0, trusted_uid=os.getuid(),
            config_path=wechat_cfg_path, contacts_path=sidecar_path,
            group_desktop_class=group_cls,
        )
        self.assertEqual(pause_res['state'], 'refused')
        self.assertEqual(pause_res['reason'], 'manual_reply_paused')
        self.assertEqual(group_cls.submits, 0)

        # Even if manual replies are explicitly resumed later, previously refused job op2 NEVER replays!
        subprocess.run([
            node_bin, '--input-type=module', '-e',
            f"""
import {{ readFileSync }} from 'node:fs';
const {{ setManualReplyControl }} = await import({json.dumps((ops_dir / 'broker.mjs').as_uri())});
const pol = JSON.parse(readFileSync({json.dumps(str(policy_path))}, 'utf8'));
setManualReplyControl(pol, {{ action: 'resume' }});
""",
        ], check=True)
        self.assertEqual(
            worker.process_job(
                wechat_dir / 'adhoc/outbox' / f"{broker_out['op2']}.json", wechat_dir, self.config,
                Queue, run_one, Desktop, clock=lambda: t0_ms / 1000.0, trusted_uid=os.getuid(),
                config_path=wechat_cfg_path, contacts_path=sidecar_path,
                group_desktop_class=group_cls,
            )['state'],
            'refused',
        )
        self.assertEqual(group_cls.submits, 0)

    def _prepare_manual_reply_deploy(self, name):
        import shutil
        import socket
        import subprocess

        deploy_root = (self.root / name).resolve()
        wechat_dir = deploy_root / 'wechat'
        source_dir = deploy_root / 'src'
        runtime_root = deploy_root / 'runtime'
        release_root = deploy_root / 'releases'
        backup_root = deploy_root / 'backups'
        state_dir = deploy_root / 'state'
        handoff_dir = deploy_root / 'handoff'
        policy_path = deploy_root / 'etc/octopus/operations.json'
        sock_path = self.root / f'{name}.sock'
        ingress_db = deploy_root / 'ingress.sqlite'
        ingress_profile = deploy_root / 'ingress/cordis.patch.yml'
        agent_profile = deploy_root / 'agent/profile'
        unit_dir = deploy_root / 'systemd'
        for d in [wechat_dir / 'config/groups/ops-group', wechat_dir / 'config/groups/bak-group',
                  wechat_dir / 'receipts/evidence', wechat_dir / 'adhoc/receipts/evidence',
                  wechat_dir / 'inbox/events', wechat_dir / 'inbox/evidence',
                  source_dir, runtime_root / 'node_modules', policy_path.parent, ingress_profile.parent, agent_profile, unit_dir]:
            d.mkdir(parents=True, exist_ok=True)
        for p, mode in [
            (deploy_root, 0o750), (wechat_dir, 0o750), (wechat_dir / 'config', 0o750),
            (wechat_dir / 'adhoc', 0o750), (wechat_dir / 'adhoc/outbox', 0o750), (wechat_dir / 'adhoc/receipts', 0o700),
            (wechat_dir / 'inbox', 0o700), (wechat_dir / 'inbox/events', 0o700), (wechat_dir / 'inbox/evidence', 0o700),
        ]:
            p.mkdir(parents=True, exist_ok=True)
            p.chmod(mode)
        for fn in installer.RELEASE_FILES:
            shutil.copyfile(Path(__file__).parents[1] / fn, source_dir / fn)
        for rel in ['config/account.png', 'config/own-chat.png', 'config/octopus-friend-label.png',
                    'config/recipient.png', 'config/recipient-sidebar.png', 'config/recipient-card.png',
                    'config/groups/ops-group/group-search.png', 'config/groups/ops-group/member-1.png',
                    'config/groups/ops-group/member-2.png', 'config/groups/ops-group/group-name.png',
                    'config/groups/bak-group/group-search.png', 'config/groups/bak-group/member-1.png']:
            shutil.copyfile(self.root / rel, wechat_dir / rel)
        (wechat_dir / 'config/config.json').write_text(json.dumps(self.config), encoding='utf-8')
        (wechat_dir / 'receipts/worker.json').write_text(json.dumps({'checked_at': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(), 'state': 'ready'}))

        repo_root = Path(__file__).parents[3]
        built_feishu_pkg = repo_root / 'packages/experimental/im-feishu'
        runtime_nm = runtime_root / 'node_modules'
        runtime_deps = {}
        for entry in (built_feishu_pkg / 'node_modules').iterdir():
            if entry.name.startswith('.'):
                continue
            if entry.name == '@deepseek-ai':
                scope_dir = runtime_nm / '@deepseek-ai'
                scope_dir.mkdir(parents=True, exist_ok=True)
                for scoped in entry.iterdir():
                    dep_name = f'@deepseek-ai/{scoped.name}'
                    if dep_name == '@deepseek-ai/dsh-experimental-im-feishu':
                        continue
                    link_target = scope_dir / scoped.name
                    if not link_target.exists():
                        link_target.symlink_to(scoped.resolve())
                    runtime_deps[dep_name] = '*'
            else:
                link_target = runtime_nm / entry.name
                if not link_target.exists():
                    link_target.symlink_to(entry.resolve())
                runtime_deps[entry.name] = '*'
        (runtime_root / 'package.json').write_text(json.dumps({'name': 'octopus-runtime', 'version': '1.0.0', 'dependencies': runtime_deps}), encoding='utf-8')

        binding_key = json.dumps(['feishu', 'app', 'org', 'chat', 'owner'], separators=(',', ':'))
        with closing(sqlite3.connect(ingress_db)) as conn:
            conn.execute('CREATE TABLE u_im_ingress_operators(key TEXT PRIMARY KEY, value TEXT)')
            conn.execute('CREATE TABLE u_im_ingress_deliveries(key TEXT PRIMARY KEY, value TEXT)')
            conn.execute('INSERT INTO u_im_ingress_operators VALUES(?,?)',
                         (binding_key, json.dumps({'operatorId': 'owner', 'targetTenantId': 'octopus', 'revokedAt': None})))
            conn.commit()
        (ingress_profile.parent / 'package.json').write_text(json.dumps({
            'name': 'octopus-ingress-profile',
            'dsh': {'profile': {'bundles': []}},
            'dependencies': {'@deepseek-ai/dsh-experimental-im-feishu': '0.2.1-alpha.1'},
        }), encoding='utf-8')
        (ingress_profile.parent / 'cordis.yml').write_text('[]\n', encoding='utf-8')
        ingress_profile.write_text(
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
        (agent_profile / 'package.json').write_text(json.dumps({'name': 'p', 'dependencies': {}}), encoding='utf-8')

        contacts_file = deploy_root / 'contacts.json'
        contacts_file.write_text(json.dumps(self.sidecar['contacts']), encoding='utf-8')

        class StopTrackCmd:
            DEVNULL = subprocess.DEVNULL
            def __init__(inner_self):
                inner_self.stop_calls = []
            def call(inner_self, cmd):
                if len(cmd) >= 3 and cmd[0] == 'systemctl' and cmd[1] == 'stop':
                    inner_self.stop_calls.append(list(cmd))
                return 0
            def check_call(inner_self, cmd):
                return 0
            def check_output(inner_self, cmd, stderr=None):
                return b'0\n'

        fake_cmd = StopTrackCmd()
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(sock_path))
        srv.listen(4)
        self.addCleanup(srv.close)
        node_bin = shutil.which('node') or '/usr/bin/node'
        return {
            'deploy_root': deploy_root, 'wechat_dir': wechat_dir, 'source_dir': source_dir,
            'runtime_root': runtime_root, 'release_root': release_root, 'backup_root': backup_root,
            'state_dir': state_dir, 'handoff_dir': handoff_dir, 'policy_path': policy_path,
            'sock_path': sock_path, 'ingress_db': ingress_db, 'ingress_profile': ingress_profile,
            'agent_profile': agent_profile, 'unit_dir': unit_dir, 'contacts_file': contacts_file,
            'built_feishu_pkg': built_feishu_pkg, 'fake_cmd': fake_cmd, 'node_bin': node_bin,
        }

    def test_end_to_end_installer_inbox_node_broker_and_python_worker_manual_reply(self):
        import hashlib
        import subprocess

        d = self._prepare_manual_reply_deploy('e2e_manual_reply')
        deploy_root, wechat_dir, source_dir = d['deploy_root'], d['wechat_dir'], d['source_dir']
        runtime_root, release_root, backup_root = d['runtime_root'], d['release_root'], d['backup_root']
        state_dir, handoff_dir, policy_path = d['state_dir'], d['handoff_dir'], d['policy_path']
        sock_path, ingress_db, ingress_profile = d['sock_path'], d['ingress_db'], d['ingress_profile']
        agent_profile, unit_dir, contacts_file = d['agent_profile'], d['unit_dir'], d['contacts_file']
        built_feishu_pkg, fake_cmd, node_bin = d['built_feishu_pkg'], d['fake_cmd'], d['node_bin']

        # Invalid manualReplyEnabled=True with enabled=False MUST fail before systemctl stop.
        with self.assertRaises((RuntimeError, ValueError)):
            installer.install(
                source_dir, runtime_root, Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=sock_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, contacts_config=contacts_file,
                notifications_config={'enabled': False, 'manualReplyEnabled': True},
                handoff_dir=handoff_dir, handoff_gid=os.getgid(), feishu_package=built_feishu_pkg,
                check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(fake_cmd.stop_calls, [])

        summary = installer.install(
            source_dir, runtime_root, Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=sock_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, contacts_config=contacts_file,
            notifications_config={
                'enabled': True, 'manualReplyEnabled': True, 'manualReplyMaxAgeSeconds': 1800,
                'inboundAggregationSeconds': 20, 'inboundProducerUid': os.getuid(),
                'receiptProducerUid': os.getuid(), 'outboxProducerUid': os.getuid(),
            },
            handoff_dir=handoff_dir, handoff_gid=os.getgid(), feishu_package=built_feishu_pkg,
            check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
        )
        self.assertEqual(summary['wechatNotifications'], {'enabled': True, 'manualReplyEnabled': True})
        installed_policy = json.loads(policy_path.read_text(encoding='utf-8'))
        self.assertTrue(installed_policy['wechatNotifications']['manualReplyEnabled'])
        self.assertEqual(installed_policy['wechatNotifications']['manualReplyMaxAgeSeconds'], 1800)
        installed_manual_view_path = wechat_dir / 'config/octopus-manual-reply.json'
        self.assertEqual(installed_manual_view_path.stat().st_mode & 0o777, 0o640)
        installed_manual_view = json.loads(installed_manual_view_path.read_text(encoding='utf-8'))
        self.assertEqual(installed_manual_view['manual_reply_enabled'], True)
        self.assertEqual(installed_manual_view['paused'], False)
        self.assertEqual(installed_manual_view['max_age_seconds'], 1800)

        # Write real inbound events for both friend-1 and group-ops with explicit observation-time binding_sha256 and evidence digests.
        now_ms = int(time.time() * 1000)
        friend_binding = worker.compute_binding_sha256(wechat_dir, self.config, self.sidecar['contacts'][0])
        group_binding = worker.compute_group_binding_sha256(wechat_dir, self.config, self.sidecar['contacts'][1])
        for ev_id, ev_name, c_kind, c_ref, trig, s_name, s_text, b_sha in [
            ('evt-e2e-friend', 'e2e_f.png', 'friend', 'friend-1', 'friend_message', '阿明', '周报今天发我吗？', friend_binding),
            ('evt-e2e-group', 'e2e_g.png', 'group', 'group-ops', 'group_message', '运维同学', '请确认上线时间', group_binding),
        ]:
            ev_bytes = f'evidence-{ev_id}'.encode('utf-8')
            ev_file = wechat_dir / 'inbox/evidence' / ev_name
            ev_file.write_bytes(ev_bytes)
            ev_file.chmod(0o600)
            ev_obj = {
                'schema_version': 1,
                'event_id': ev_id,
                'account_sha256': worker.sha('own-id'),
                'trigger_type': trig,
                'conversation_kind': c_kind,
                'conversation_ref': c_ref,
                'binding_sha256': b_sha,
                'sender_name': s_name,
                'source_ref': f'src-{ev_id}',
                'source_time': '16:00',
                'observed_at': now_ms - 30_000,
                'text': s_text,
                'evidence_path': f'/home/wechat/.gamma-moments/inbox/evidence/{ev_name}',
                'evidence_sha256': hashlib.sha256(ev_bytes).hexdigest(),
            }
            ev_path = wechat_dir / 'inbox/events' / f'{ev_id}.json'
            ev_path.write_text(json.dumps(ev_obj), encoding='utf-8')
            ev_path.chmod(0o600)

        def run_node(script):
            env = {**os.environ, 'POLICY_PATH': str(policy_path), 'RELEASE_CUR': str(release_root / 'current'),
                   'INGRESS_DB': str(ingress_db)}
            res = subprocess.run([node_bin, '--input-type=module', '-e', script], capture_output=True, text=True, env=env)
            if res.returncode != 0:
                self.fail(f'node script failed ({res.returncode}):\nstdout={res.stdout}\nstderr={res.stderr}')
            return json.loads(res.stdout)

        in_res = run_node(f"""
import {{ readFileSync }} from 'node:fs';
import {{ pathToFileURL }} from 'node:url';
import {{ join }} from 'node:path';
const cur = process.env.RELEASE_CUR;
const {{ validatePolicy }} = await import(pathToFileURL(join(cur, 'policy.mjs')).href);
const {{ Operations }} = await import(pathToFileURL(join(cur, 'operations.mjs')).href);
const {{ dispatch, perform }} = await import(pathToFileURL(join(cur, 'broker.mjs')).href);
const {{ WechatExchangeProjector }} = await import(pathToFileURL(join(cur, 'wechat-inbox.mjs')).href);
import {{ DatabaseSync }} from 'node:sqlite';
let clock = {now_ms};
const policy = validatePolicy(JSON.parse(readFileSync(process.env.POLICY_PATH, 'utf8')));
const j = new Operations(policy, process.env.POLICY_PATH, perform, () => clock);
const proj = new WechatExchangeProjector(policy, {{ clock: () => clock }});
const batchRes = proj.projectOnce();
proj.close();
const friendNotice = batchRes.sealed.find(b => b.conversationKey === 'friend:friend-1').noticeId;
const groupNotice = batchRes.sealed.find(b => b.conversationKey === 'group:group-ops').noticeId;

const db = new DatabaseSync(process.env.INGRESS_DB);
function setPrompt(id, prompt) {{
  db.prepare('DELETE FROM u_im_ingress_deliveries').run();
  db.prepare('INSERT INTO u_im_ingress_deliveries VALUES(?,?)').run('c', JSON.stringify({{
    status: 'in_flight', operatorId: 'owner', targetTenantId: 'octopus', requestId: id,
    prompt, receivedAt: new Date(clock).toISOString(),
    deliveryKey: JSON.stringify(['feishu','app','org','chat','owner',id]),
  }}));
}}
setPrompt('req-e2e-f', `回复 ${{friendNotice}}：周报五点前发你`);
const fOp = j.prepare({{ kind: 'wechat_reply_notice', payload: {{ noticeId: friendNotice, text: '周报五点前发你' }} }});
await dispatch({{ action: 'execute', params: {{ id: fOp.id }} }}, j);
setPrompt('req-e2e-g', `回复 ${{groupNotice}}：今晚八点上线`);
const gOp = j.prepare({{ kind: 'wechat_reply_notice', payload: {{ noticeId: groupNotice, text: '今晚八点上线' }} }});
await dispatch({{ action: 'execute', params: {{ id: gOp.id }} }}, j);
j.close();
db.close();
console.log(JSON.stringify({{ friendNotice, groupNotice, fId: fOp.id, gId: gOp.id }}));
""")

        installed_cfg = json.loads((wechat_dir / 'config/config.json').read_text(encoding='utf-8'))
        installed_sidecar = wechat_dir / 'config/octopus-contacts.json'
        Desktop.submissions = []
        Desktop.fail_after_submit = False
        try:
            f_worker_res = worker.process_job(
                wechat_dir / 'adhoc/outbox' / f"{in_res['fId']}.json", wechat_dir, installed_cfg,
                Queue, run_one, Desktop, trusted_uid=os.getuid(),
                config_path=wechat_dir / 'config/config.json', contacts_path=installed_sidecar,
            )
            g_worker_res = worker.process_job(
                wechat_dir / 'adhoc/outbox' / f"{in_res['gId']}.json", wechat_dir, installed_cfg,
                Queue, run_one, Desktop, trusted_uid=os.getuid(),
                config_path=wechat_dir / 'config/config.json', contacts_path=installed_sidecar,
                group_desktop_class=self._make_group_desktop_cls(),
            )
            self.assertEqual(f_worker_res['state'], 'sent_verified')
            self.assertEqual(g_worker_res['state'], 'sent_verified')
            self.assertEqual(Desktop.submissions, ['周报五点前发你'])
        finally:
            Desktop.submissions = []
            Desktop.fail_after_submit = False

        out_res = run_node(f"""
import {{ readFileSync }} from 'node:fs';
import {{ pathToFileURL }} from 'node:url';
import {{ join }} from 'node:path';
const cur = process.env.RELEASE_CUR;
const {{ validatePolicy }} = await import(pathToFileURL(join(cur, 'policy.mjs')).href);
const {{ Operations }} = await import(pathToFileURL(join(cur, 'operations.mjs')).href);
const {{ dispatch, perform }} = await import(pathToFileURL(join(cur, 'broker.mjs')).href);
const {{ WechatExchangeProjector }} = await import(pathToFileURL(join(cur, 'wechat-inbox.mjs')).href);
const policy = validatePolicy(JSON.parse(readFileSync(process.env.POLICY_PATH, 'utf8')));
const proj = new WechatExchangeProjector(policy, {{ clock: () => {now_ms + 5000} }});
const receiptProj = proj.projectOnce();
proj.close();
const j = new Operations(policy, process.env.POLICY_PATH, perform);
const rFriend = await dispatch({{ action: 'receipt', params: {{ id: {json.dumps(in_res['fId'])} }} }}, j);
const rGroup = await dispatch({{ action: 'receipt', params: {{ id: {json.dumps(in_res['gId'])} }} }}, j);
j.close();
console.log(JSON.stringify({{ receiptProj, rFriend, rGroup }}));
""")
        self.assertEqual(out_res['receiptProj']['receipts']['projected'], 2)
        self.assertEqual(out_res['receiptProj']['receipts']['rejected'], [])
        self.assertEqual(out_res['rFriend']['state'], 'completed')
        self.assertEqual(out_res['rFriend']['workerReceipt']['state'], 'sent_verified')
        self.assertEqual(out_res['rGroup']['state'], 'completed')
        self.assertEqual(out_res['rGroup']['workerReceipt']['state'], 'sent_verified')
        handoff_kinds = [json.loads(p.read_text(encoding='utf-8'))['kind'] for p in sorted(handoff_dir.glob('*.json'))]
        self.assertEqual(handoff_kinds.count('inbound_alert'), 2)
        self.assertEqual(handoff_kinds.count('outbound_receipt'), 2)

        # Pausing manual replies via broker config view halts already-queued jobs BEFORE UI launch, and re-installing persists the paused state!
        queued_pause_res = run_node(f"""
import {{ readFileSync }} from 'node:fs';
import {{ pathToFileURL }} from 'node:url';
import {{ join }} from 'node:path';
import {{ DatabaseSync }} from 'node:sqlite';
const cur = process.env.RELEASE_CUR;
const {{ validatePolicy }} = await import(pathToFileURL(join(cur, 'policy.mjs')).href);
const {{ Operations }} = await import(pathToFileURL(join(cur, 'operations.mjs')).href);
const {{ dispatch, perform, setManualReplyControl }} = await import(pathToFileURL(join(cur, 'broker.mjs')).href);
const policy = validatePolicy(JSON.parse(readFileSync(process.env.POLICY_PATH, 'utf8')));
const clock = {now_ms};
const db = new DatabaseSync(process.env.INGRESS_DB);
db.prepare('DELETE FROM u_im_ingress_deliveries').run();
db.prepare('INSERT INTO u_im_ingress_deliveries VALUES(?,?)').run('c', JSON.stringify({{
  status: 'in_flight', operatorId: 'owner', targetTenantId: 'octopus', requestId: 'req-e2e-pause',
  prompt: '回复 ' + {json.dumps(in_res['friendNotice'])} + '：待暂停队列项',
  receivedAt: new Date(clock).toISOString(),
  deliveryKey: JSON.stringify(['feishu','app','org','chat','owner','req-e2e-pause']),
}}));
const j = new Operations(policy, process.env.POLICY_PATH, perform, () => clock);
const op = j.prepare({{ kind: 'wechat_reply_notice', payload: {{ noticeId: {json.dumps(in_res['friendNotice'])}, text: '待暂停队列项' }} }});
await dispatch({{ action: 'execute', params: {{ id: op.id }} }}, j);
j.close();
db.close();
setManualReplyControl(policy, {{ action: 'pause', nowMs: clock }});
console.log(JSON.stringify({{ pausedOpId: op.id }}));
""")
        paused_view = json.loads(installed_manual_view_path.read_text(encoding='utf-8'))
        self.assertEqual(paused_view['manual_reply_enabled'], True)
        self.assertEqual(paused_view['paused'], True)
        Desktop.submissions = []
        paused_worker_res = worker.process_job(
            wechat_dir / 'adhoc/outbox' / f"{queued_pause_res['pausedOpId']}.json", wechat_dir, installed_cfg,
            Queue, run_one, Desktop, trusted_uid=os.getuid(),
            config_path=wechat_dir / 'config/config.json', contacts_path=installed_sidecar,
        )
        self.assertEqual(paused_worker_res['state'], 'refused')
        self.assertEqual(paused_worker_res['reason'], 'manual_reply_paused')
        self.assertEqual(Desktop.submissions, [])

        # After queued work settles, installer.install with manualReplyEnabled=False also tightens manual_reply_enabled cleanly.
        summary_paused = installer.install(
            source_dir, runtime_root, Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=sock_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, contacts_config=contacts_file,
            notifications_config={
                'enabled': True, 'manualReplyEnabled': False, 'inboundAggregationSeconds': 20,
                'inboundProducerUid': os.getuid(), 'receiptProducerUid': os.getuid(), 'outboxProducerUid': os.getuid(),
            },
            handoff_dir=handoff_dir, handoff_gid=os.getgid(), feishu_package=built_feishu_pkg,
            check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
        )
        self.assertEqual(summary_paused['wechatNotifications'], {'enabled': True})
        persisted_paused_view = json.loads(installed_manual_view_path.read_text(encoding='utf-8'))
        self.assertEqual(persisted_paused_view['manual_reply_enabled'], False)
        self.assertEqual(persisted_paused_view['paused'], True)

    def test_production_broker_poll_restart_and_ordinary_reinstall_preserve_pause_and_finite_lease(self):
        import hashlib
        import subprocess

        d = self._prepare_manual_reply_deploy('e2e_pause_preserve')
        wechat_dir, source_dir, runtime_root = d['wechat_dir'], d['source_dir'], d['runtime_root']
        release_root, backup_root, state_dir = d['release_root'], d['backup_root'], d['state_dir']
        handoff_dir, policy_path, sock_path = d['handoff_dir'], d['policy_path'], d['sock_path']
        ingress_db, ingress_profile, agent_profile = d['ingress_db'], d['ingress_profile'], d['agent_profile']
        unit_dir, contacts_file = d['unit_dir'], d['contacts_file']
        built_feishu_pkg, fake_cmd, node_bin = d['built_feishu_pkg'], d['fake_cmd'], d['node_bin']

        installer.install(
            source_dir, runtime_root, Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=sock_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, contacts_config=contacts_file,
            notifications_config={
                'enabled': True, 'manualReplyEnabled': True, 'manualReplyMaxAgeSeconds': 3600,
                'inboundAggregationSeconds': 20, 'inboundProducerUid': os.getuid(),
                'receiptProducerUid': os.getuid(), 'outboxProducerUid': os.getuid(),
            },
            handoff_dir=handoff_dir, handoff_gid=os.getgid(), feishu_package=built_feishu_pkg,
            check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
        )

        # Write one valid friend inbound event with observation-time binding_sha256, queue BOTH a reply job AND a normal non-reply send job!
        now_ms = int(time.time() * 1000)
        friend_binding = worker.compute_binding_sha256(wechat_dir, self.config, self.sidecar['contacts'][0])
        ev_bytes = b'pause-preserve-ev'
        (wechat_dir / 'inbox/evidence/p_f.png').write_bytes(ev_bytes)
        (wechat_dir / 'inbox/evidence/p_f.png').chmod(0o600)
        ev_file = wechat_dir / 'inbox/events/evt-p-friend.json'
        ev_file.write_text(json.dumps({
            'schema_version': 1,
            'event_id': 'evt-p-friend',
            'account_sha256': worker.sha('own-id'),
            'trigger_type': 'friend_message',
            'conversation_kind': 'friend',
            'conversation_ref': 'friend-1',
            'binding_sha256': friend_binding,
            'sender_name': '阿明',
            'source_ref': 'src-pf-1',
            'source_time': '18:10',
            'observed_at': now_ms - 25_000,
            'text': '请确认周报',
            'evidence_path': '/home/wechat/.gamma-moments/inbox/evidence/p_f.png',
            'evidence_sha256': hashlib.sha256(ev_bytes).hexdigest(),
        }), encoding='utf-8')
        ev_file.chmod(0o600)

        queued = json.loads(subprocess.check_output([
            node_bin, '--input-type=module', '-e',
            f"""
import {{ readFileSync }} from 'node:fs';
import {{ pathToFileURL }} from 'node:url';
import {{ join }} from 'node:path';
import {{ DatabaseSync }} from 'node:sqlite';
const cur = {json.dumps(str(release_root / 'current'))};
const {{ validatePolicy }} = await import(pathToFileURL(join(cur, 'policy.mjs')).href);
const {{ Operations }} = await import(pathToFileURL(join(cur, 'operations.mjs')).href);
const {{ dispatch, perform }} = await import(pathToFileURL(join(cur, 'broker.mjs')).href);
const {{ WechatExchangeProjector }} = await import(pathToFileURL(join(cur, 'wechat-inbox.mjs')).href);
const policy = validatePolicy(JSON.parse(readFileSync({json.dumps(str(policy_path))}, 'utf8')));
const clock = {now_ms};
const j = new Operations(policy, {json.dumps(str(policy_path))}, perform, () => clock);
const proj = new WechatExchangeProjector(policy, {{ clock: () => clock }});
const sealed = proj.projectOnce().sealed;
proj.close();
const nid = sealed[0].noticeId;
const db = new DatabaseSync({json.dumps(str(ingress_db))});
function setPrompt(id, prompt) {{
  db.prepare('DELETE FROM u_im_ingress_deliveries').run();
  db.prepare('INSERT INTO u_im_ingress_deliveries VALUES(?,?)').run('c', JSON.stringify({{
    status: 'in_flight', operatorId: 'owner', targetTenantId: 'octopus', requestId: id,
    prompt, receivedAt: new Date(clock).toISOString(),
    deliveryKey: JSON.stringify(['feishu','app','org','chat','owner',id]),
  }}));
}}
setPrompt('req-p-reply', `回复 ${{nid}}：暂停保真队列验证`);
const rOp = j.prepare({{ kind: 'wechat_reply_notice', payload: {{ noticeId: nid, text: '暂停保真队列验证' }} }});
await dispatch({{ action: 'execute', params: {{ id: rOp.id }} }}, j);
setPrompt('req-p-send', '给好友一发微信：普通主动发送验证');
const sOp = j.prepare({{ kind: 'wechat_send', payload: {{ recipient: '好友一', text: '普通主动发送验证' }} }});
await dispatch({{ action: 'execute', params: {{ id: sOp.id }} }}, j);
j.close();
db.close();
console.log(JSON.stringify({{ replyId: rOp.id, sendId: sOp.id }}));
""",
        ], text=True))

        # Apply explicit finite lease + reduced max_age=60 + pause through documented broker CLI.
        broker_entry = str(release_root / 'current/broker.mjs')
        resumed = json.loads(subprocess.check_output(
            [node_bin, broker_entry, str(policy_path), 'manual-reply', 'resume', '--max-age-seconds', '60', '--lease-seconds', '180'],
            text=True,
        ))
        paused = json.loads(subprocess.check_output(
            [node_bin, broker_entry, str(policy_path), 'manual-reply', 'pause'],
            text=True,
        ))
        finite_lease = resumed['view']['lease_expires_at']
        self.assertEqual(paused['view']['paused'], True)
        self.assertEqual(paused['view']['max_age_seconds'], 60)
        self.assertEqual(paused['view']['lease_expires_at'], finite_lease)

        # Simulate normal broker poll (createBrokerProjectionLoop.start) AND broker restart (reconcileManualReplyConfigView):
        # temporary pause, max_age=60, and finite lease MUST NOT disappear!
        subprocess.check_call([
            node_bin, '--input-type=module', '-e',
            f"""
import {{ readFileSync }} from 'node:fs';
import {{ pathToFileURL }} from 'node:url';
import {{ join }} from 'node:path';
const cur = {json.dumps(str(release_root / 'current'))};
const {{ validatePolicy }} = await import(pathToFileURL(join(cur, 'policy.mjs')).href);
const {{ createBrokerProjectionLoop, reconcileManualReplyConfigView }} = await import(pathToFileURL(join(cur, 'broker.mjs')).href);
const policy = validatePolicy(JSON.parse(readFileSync({json.dumps(str(policy_path))}, 'utf8')));
reconcileManualReplyConfigView(policy);
const loop = createBrokerProjectionLoop(policy, {{ policyPath: {json.dumps(str(policy_path))} }});
loop.start();
loop.stop();
""",
        ])
        view_path = wechat_dir / 'config/octopus-manual-reply.json'
        after_poll = json.loads(view_path.read_text(encoding='utf-8'))
        self.assertEqual(after_poll['paused'], True)
        self.assertEqual(after_poll['max_age_seconds'], 60)
        self.assertEqual(after_poll['lease_expires_at'], finite_lease)

        # Verify worker refuses the queued reply task while paused, AND still delivers the queued ordinary non-reply send!
        installed_cfg = json.loads((wechat_dir / 'config/config.json').read_text(encoding='utf-8'))
        installed_sidecar = wechat_dir / 'config/octopus-contacts.json'
        Desktop.submissions = []
        Desktop.fail_after_submit = False
        try:
            r_res = worker.process_job(
                wechat_dir / 'adhoc/outbox' / f"{queued['replyId']}.json", wechat_dir, installed_cfg,
                Queue, run_one, Desktop, trusted_uid=os.getuid(),
                config_path=wechat_dir / 'config/config.json', contacts_path=installed_sidecar,
            )
            s_res = worker.process_job(
                wechat_dir / 'adhoc/outbox' / f"{queued['sendId']}.json", wechat_dir, installed_cfg,
                Queue, run_one, Desktop, trusted_uid=os.getuid(),
                config_path=wechat_dir / 'config/config.json', contacts_path=installed_sidecar,
            )
            self.assertEqual(r_res['state'], 'refused')
            self.assertEqual(r_res['reason'], 'manual_reply_paused')
            self.assertEqual(s_res['state'], 'sent_verified')
            self.assertEqual(Desktop.submissions, ['普通主动发送验证'])
        finally:
            Desktop.submissions = []

        # Ordinary reinstall (EVEN with notifications_config={'enabled': True, 'manualReplyEnabled': True, 'manualReplyMaxAgeSeconds': 3600})
        # WITHOUT --resume-manual-reply MUST preserve paused=True, max_age_seconds=60, and finite lease!
        installer.install(
            source_dir, runtime_root, Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=sock_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, contacts_config=contacts_file,
            notifications_config={
                'enabled': True, 'manualReplyEnabled': True, 'manualReplyMaxAgeSeconds': 3600,
                'inboundAggregationSeconds': 20, 'inboundProducerUid': os.getuid(),
                'receiptProducerUid': os.getuid(), 'outboxProducerUid': os.getuid(),
            },
            handoff_dir=handoff_dir, handoff_gid=os.getgid(), feishu_package=built_feishu_pkg,
            check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
        )
        after_reinstall = json.loads(view_path.read_text(encoding='utf-8'))
        self.assertEqual(after_reinstall['paused'], True)
        self.assertEqual(after_reinstall['max_age_seconds'], 60)
        self.assertEqual(after_reinstall['lease_expires_at'], finite_lease)

        # Explicit resume with finite 1s lease clears pause & sets finite lease, and worker blocks once lease expires!
        installer.install(
            source_dir, runtime_root, Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=sock_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, contacts_config=contacts_file,
            notifications_config={
                'enabled': True, 'manualReplyEnabled': True, 'manualReplyMaxAgeSeconds': 3600,
                'inboundAggregationSeconds': 20, 'inboundProducerUid': os.getuid(),
                'receiptProducerUid': os.getuid(), 'outboxProducerUid': os.getuid(),
            },
            handoff_dir=handoff_dir, handoff_gid=os.getgid(), feishu_package=built_feishu_pkg,
            resume_manual_reply=True, manual_reply_lease_seconds=1,
            check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
        )
        resumed_view = json.loads(view_path.read_text(encoding='utf-8'))
        self.assertEqual(resumed_view['paused'], False)
        self.assertEqual(resumed_view['max_age_seconds'], 3600)
        self.assertIsNotNone(resumed_view['lease_expires_at'])
        lease_exp_job = self._friend_job(
            '77' * 12, text='租约到期拦截验证', origin_notice_id=True,
            created_at=now_ms, expires_at=now_ms + 600_000,
        )
        try:
            Desktop.submissions = []
            expired_lease_res = worker.process_job(
                lease_exp_job, wechat_dir, installed_cfg, Queue, run_one, Desktop,
                clock=lambda: (resumed_view['lease_expires_at'] + 500) / 1000.0,
                trusted_uid=os.getuid(), config_path=wechat_dir / 'config/config.json',
                contacts_path=installed_sidecar,
            )
            self.assertEqual(expired_lease_res['state'], 'refused')
            self.assertEqual(expired_lease_res['reason'], 'manual_reply_lease_expired')
            self.assertEqual(Desktop.submissions, [])
        finally:
            Desktop.submissions = []

        # Corrupted octopus-manual-reply.json MUST fail closed BEFORE systemctl stop on ordinary reinstall!
        saved_view_raw = view_path.read_text(encoding='utf-8')
        view_path.write_text('{corrupt', encoding='utf-8')
        fake_cmd.stop_calls.clear()
        with self.assertRaises(RuntimeError):
            installer.install(
                source_dir, runtime_root, Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=sock_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, contacts_config=contacts_file,
                handoff_dir=handoff_dir, handoff_gid=os.getgid(), feishu_package=built_feishu_pkg,
                check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(fake_cmd.stop_calls, [])
        self.assertEqual(view_path.read_text(encoding='utf-8'), '{corrupt')
        view_path.write_text(saved_view_raw, encoding='utf-8')


class NotificationProjectionInstallTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_install_includes_wechat_inbox_defaults_off_and_supports_rollback_when_enabled(self):
        import shutil
        import socket
        import subprocess

        self.assertIn('wechat-inbox.mjs', installer.RELEASE_FILES)

        deploy_root = self.root / 'r'
        wechat_dir = deploy_root / 'wechat'
        release_root = deploy_root / 'releases-root'
        backup_root = deploy_root / 'backups'
        state_dir = deploy_root / 'state'
        handoff_dir = deploy_root / 'handoff'
        policy_path = deploy_root / 'etc/octopus/operations.json'
        socket_path = self.root / 'b.sock'
        ingress_db = deploy_root / 'ingress/state/ingress.sqlite'
        ingress_profile = deploy_root / 'ingress/cordis.patch.yml'
        agent_profile = deploy_root / 'agent/profile'
        unit_dir = deploy_root / 'systemd'
        source_dir = deploy_root / 'src'
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
        }))
        binding_key = json.dumps(['feishu', 'cli_app', 'org', 'oc_chat', 'ou_owner'], separators=(',', ':'))
        initial_ingress_yaml = (
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
            '          kind: feishu\n'
        )
        ingress_profile.write_text(initial_ingress_yaml, encoding='utf-8')
        (agent_profile / 'cordis.patch.yml').write_text(json.dumps([{'id': 'im-runtime', 'inject': ['agents']}]))
        (agent_profile / 'package.json').write_text(json.dumps({'name': 'octopus-profile', 'dependencies': {}}))

        src_dir = Path(__file__).parents[1]
        repo_root = Path(__file__).parents[3]
        built_feishu_pkg = repo_root / 'packages/experimental/im-feishu'
        runtime_root = deploy_root / 'runtime'
        for fname in installer.RELEASE_FILES:
            (source_dir / fname).write_text((src_dir / fname).read_text(encoding='utf-8'), encoding='utf-8')
        runtime_root.mkdir(parents=True, exist_ok=True)
        (runtime_root / 'package.json').write_text(json.dumps({
            'name': 'octopus-runtime',
            'version': '1.0.0',
            'dependencies': {},
        }))
        (ingress_profile.parent / 'package.json').write_text(json.dumps({
            'name': 'octopus-ingress-profile',
            'dsh': {'profile': {'bundles': []}},
            'dependencies': {'@deepseek-ai/dsh-experimental-im-feishu': '0.2.1-alpha.1'},
        }))
        (ingress_profile.parent / 'cordis.yml').write_text('[]\n', encoding='utf-8')

        def write_old_runtime_feishu(pkg_dir: Path, *, with_built_entry: bool = True):
            pkg_dir.mkdir(parents=True, exist_ok=True)
            (pkg_dir / 'package.json').write_text(json.dumps({
                'name': '@deepseek-ai/dsh-experimental-im-feishu',
                'version': '0.2.1-alpha.1',
                'type': 'module',
                'exports': {'./ingress': './lib/ingress-plugin.js'},
            }))
            if with_built_entry:
                lib_dir = pkg_dir / 'lib'
                lib_dir.mkdir(parents=True, exist_ok=True)
                (lib_dir / 'ingress-plugin.js').write_text(
                    'export async function apply() {}\n'
                    'export const ingressPluginConfigSchema = {\n'
                    '  safeParse(cfg) {\n'
                    '    if (cfg && typeof cfg === "object" && "ownerNotices" in cfg) {\n'
                    '      return { success: false, error: { message: "unrecognized key ownerNotices" } };\n'
                    '    }\n'
                    '    return { success: true, data: cfg };\n'
                    '  },\n'
                    '};\n',
                    encoding='utf-8',
                )

        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(socket_path))
        srv.listen(4)
        self.addCleanup(srv.close)

        class FakeCmd:
            DEVNULL = subprocess.DEVNULL
            fail_ingress_restart = False
            stop_calls = []
            services = {
                'octopus-operations.service': True,
                'gamma-wechat-moments-worker.service': True,
                'octopus-ingress.service': True,
            }

            def call(self, cmd):
                if len(cmd) >= 3 and cmd[0] == 'systemctl' and cmd[1] == 'stop':
                    self.stop_calls.append(list(cmd))
                if len(cmd) >= 3 and cmd[0] == 'systemctl' and cmd[1] in {'is-active', 'is-enabled'}:
                    return 0 if self.services.get(cmd[-1], False) else 3
                return 0

            def check_call(self, cmd):
                if len(cmd) >= 3 and cmd[0] == 'systemctl' and cmd[1] == 'stop':
                    self.stop_calls.append(list(cmd))
                if cmd[:3] == ['systemctl', 'restart', 'octopus-ingress.service'] and self.fail_ingress_restart:
                    raise subprocess.CalledProcessError(1, cmd)
                return 0

            def check_output(self, cmd, stderr=None):
                return b'0\n'

        fake_cmd = FakeCmd()
        node_bin = shutil.which('node') or '/usr/bin/node'
        initial_pkg_json = (ingress_profile.parent / 'package.json').read_text(encoding='utf-8')

        # 1. Default install keeps wechatNotifications disabled without needing feishu package.
        summary_default = installer.install(
            source_dir, runtime_root, Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
        )
        self.assertEqual(summary_default['wechatNotifications'], {'enabled': False})
        self.assertNotIn('feishuPackage', summary_default)
        self.assertTrue((release_root / 'current/wechat-inbox.mjs').is_file())
        self.assertEqual(ingress_profile.read_text(encoding='utf-8'), initial_ingress_yaml)
        self.assertFalse(handoff_dir.exists())

        # 2a. Enabling notifications with missing ingress package fails closed in Phase A before systemctl stop.
        fake_cmd.stop_calls.clear()
        with self.assertRaises(RuntimeError) as missing_exc:
            installer.install(
                source_dir, runtime_root, Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, notifications_config={'enabled': True, 'inboundAggregationSeconds': 20},
                handoff_dir=handoff_dir, handoff_gid=os.getgid(),
                check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertIn('capability preflight failed', str(missing_exc.exception))
        self.assertEqual(fake_cmd.stop_calls, [])
        self.assertEqual(ingress_profile.read_text(encoding='utf-8'), initial_ingress_yaml)

        # 2b. Old runtime ingress with identical 0.2.1-alpha.1 semver but missing ownerNoticesCapability fails closed in Phase A.
        runtime_nm = runtime_root / 'node_modules'
        runtime_nm.mkdir(parents=True, exist_ok=True)
        runtime_deps = {}
        feishu_nm = built_feishu_pkg / 'node_modules'
        for entry in feishu_nm.iterdir():
            if entry.name.startswith('.'):
                continue
            if entry.name == '@deepseek-ai':
                scope_dir = runtime_nm / '@deepseek-ai'
                scope_dir.mkdir(parents=True, exist_ok=True)
                for scoped in entry.iterdir():
                    dep_name = f'@deepseek-ai/{scoped.name}'
                    if dep_name == '@deepseek-ai/dsh-experimental-im-feishu':
                        continue
                    link_target = scope_dir / scoped.name
                    if not link_target.exists():
                        link_target.symlink_to(scoped.resolve())
                    runtime_deps[dep_name] = '*'
            else:
                link_target = runtime_nm / entry.name
                if not link_target.exists():
                    link_target.symlink_to(entry.resolve())
                runtime_deps[entry.name] = '*'
        old_runtime_feishu = runtime_root / 'node_modules/@deepseek-ai/dsh-experimental-im-feishu'
        write_old_runtime_feishu(old_runtime_feishu, with_built_entry=True)
        runtime_deps['@deepseek-ai/dsh-experimental-im-feishu'] = '0.2.1-alpha.1'
        (runtime_root / 'package.json').write_text(json.dumps({
            'name': 'octopus-runtime',
            'version': '1.0.0',
            'dependencies': runtime_deps,
        }))
        fake_cmd.stop_calls.clear()
        with self.assertRaises(RuntimeError) as old_exc:
            installer.install(
                source_dir, runtime_root, Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, notifications_config={'enabled': True, 'inboundAggregationSeconds': 20},
                handoff_dir=handoff_dir, handoff_gid=os.getgid(),
                check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertIn('ownerNoticesCapability', str(old_exc.exception))
        self.assertEqual(fake_cmd.stop_calls, [])
        self.assertEqual(ingress_profile.read_text(encoding='utf-8'), initial_ingress_yaml)
        self.assertEqual((ingress_profile.parent / 'package.json').read_text(encoding='utf-8'), initial_pkg_json)

        # 2c. Source-only --feishu-package (missing lib/ or pointing at .ts instead of built JS) fails closed in Phase A.
        unbuilt_feishu = deploy_root / 'unbuilt-feishu'
        unbuilt_feishu.mkdir(parents=True, exist_ok=True)
        (unbuilt_feishu / 'package.json').write_text(json.dumps({
            'name': '@deepseek-ai/dsh-experimental-im-feishu',
            'version': '0.2.1-alpha.1',
            'type': 'module',
            'exports': {'./ingress': './src/ingress-plugin.ts'},
        }))
        (unbuilt_feishu / 'src').mkdir(parents=True, exist_ok=True)
        (unbuilt_feishu / 'src/ingress-plugin.ts').write_text('export const apply = () => {}\n', encoding='utf-8')
        fake_cmd.stop_calls.clear()
        with self.assertRaises(RuntimeError) as source_exc:
            installer.install(
                source_dir, runtime_root, Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, notifications_config={'enabled': True, 'inboundAggregationSeconds': 20},
                handoff_dir=handoff_dir, handoff_gid=os.getgid(),
                feishu_package=unbuilt_feishu,
                check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertIn('built lib/', str(source_exc.exception))
        (unbuilt_feishu / 'lib').mkdir(parents=True, exist_ok=True)
        (unbuilt_feishu / 'lib/ingress-plugin.js').write_text('export const apply = () => {}\n', encoding='utf-8')
        (unbuilt_feishu / 'lib/index.js').write_text('export {}\n', encoding='utf-8')
        with self.assertRaises(RuntimeError) as ts_export_exc:
            installer.install(
                source_dir, runtime_root, Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, notifications_config={'enabled': True, 'inboundAggregationSeconds': 20},
                handoff_dir=handoff_dir, handoff_gid=os.getgid(),
                feishu_package=unbuilt_feishu,
                check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertIn('must be built JS', str(ts_export_exc.exception))
        self.assertEqual(fake_cmd.stop_calls, [])

        # 2d. Invalid notifications_config fails in Phase A before modifying ingress_profile or services.
        with self.assertRaises(RuntimeError):
            installer.install(
                source_dir, runtime_root, Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, notifications_config={'enabled': True, 'inboundAggregationSeconds': 5},
                handoff_dir=handoff_dir, handoff_gid=os.getgid(),
                feishu_package=built_feishu_pkg,
                check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(ingress_profile.read_text(encoding='utf-8'), initial_ingress_yaml)

        # 3. Failed ingress restart during Phase B rolls back ingress_profile, package.json, symlink, and newly staged feishu package cleanly.
        fake_cmd.fail_ingress_restart = True
        with self.assertRaises(subprocess.CalledProcessError):
            installer.install(
                source_dir, runtime_root, Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, notifications_config={'enabled': True, 'inboundAggregationSeconds': 20},
                handoff_dir=handoff_dir, handoff_gid=os.getgid(),
                feishu_package=built_feishu_pkg,
                check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertEqual(ingress_profile.read_text(encoding='utf-8'), initial_ingress_yaml)
        self.assertEqual((ingress_profile.parent / 'package.json').read_text(encoding='utf-8'), initial_pkg_json)
        self.assertFalse((ingress_profile.parent / 'node_modules/@deepseek-ai/dsh-experimental-im-feishu').exists())
        feishu_pkg_root = release_root / 'feishu-packages'
        self.assertTrue(not feishu_pkg_root.exists() or list(feishu_pkg_root.iterdir()) == [])

        # 4. Enabled notifications install with offline --feishu-package overrides old 0.2.1-alpha.1 runtime package
        # and matches real @deepseek-ai/dsh-app-boot launcher resolution.
        fake_cmd.fail_ingress_restart = False
        chown_log = []
        summary_enabled = installer.install(
            source_dir, runtime_root, Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, notifications_config={'enabled': True, 'inboundAggregationSeconds': 20, 'pollIntervalMs': 2500},
            handoff_dir=handoff_dir, handoff_gid=22005,
            feishu_package=built_feishu_pkg,
            check_root=False, run_cmd=fake_cmd, chown_fn=lambda p, u, g: chown_log.append((str(p), u, g)),
        )
        self.assertEqual(summary_enabled['wechatNotifications'], {'enabled': True})
        fp_info = summary_enabled['feishuPackage']
        self.assertIsNotNone(fp_info)
        self.assertEqual(fp_info['scope'], 'profile-local')
        staged_pkg_dir = Path(fp_info['entryPath']).parents[1]
        profile_pkg_copy = ingress_profile.parent / 'node_modules/@deepseek-ai/dsh-experimental-im-feishu'
        self.assertFalse(profile_pkg_copy.is_symlink())
        self.assertTrue(profile_pkg_copy.is_dir())
        self.assertEqual(staged_pkg_dir.resolve(), profile_pkg_copy.resolve())
        self.assertTrue((profile_pkg_copy / 'lib/ingress-plugin.js').is_file())
        self.assertFalse((profile_pkg_copy / 'node_modules').exists())
        archive_dirs = list((release_root / 'feishu-packages').iterdir())
        self.assertEqual(len(archive_dirs), 1)
        self.assertTrue((archive_dirs[0] / 'lib/ingress-plugin.js').is_file())
        pinned_pkg_json = json.loads((ingress_profile.parent / 'package.json').read_text(encoding='utf-8'))
        self.assertEqual(
            pinned_pkg_json['dependencies']['@deepseek-ai/dsh-experimental-im-feishu'],
            json.loads((profile_pkg_copy / 'package.json').read_text(encoding='utf-8'))['version'],
        )

        # Prove real @deepseek-ai/dsh-app-boot launcher selects profile_pkg_copy over old_runtime_feishu
        # and resolves its runtime dependencies from runtime_root via active profile interception.
        parity_script = (
            "import { createRequire } from 'node:module';\n"
            "import { pathToFileURL } from 'node:url';\n"
            "import { Context } from '@deepseek-ai/cordis';\n"
            "import { loadProfileDirectory, createRuntimeResolution, PluginPackages } from '@deepseek-ai/dsh-app-boot';\n"
            f"const profileDir = {json.dumps(str(ingress_profile.parent))};\n"
            f"const installAnchor = {json.dumps(str(runtime_root / 'package.json'))};\n"
            f"const homeDir = {json.dumps(str(deploy_root / 'dsh-home'))};\n"
            "const profile = loadProfileDirectory('dsh-test', profileDir, installAnchor);\n"
            "const resolution = await createRuntimeResolution({ installAnchor, profile, home: homeDir });\n"
            "const ctx = new Context();\n"
            "await ctx.plugin(PluginPackages, { resolution });\n"
            "try {\n"
            "  const entrySpec = '@deepseek-ai/dsh-experimental-im-feishu/ingress';\n"
            "  const parentUrl = pathToFileURL(profileDir + '/cordis.patch.yml').href;\n"
            "  const pkg = ctx.pluginPackages.packageOf(entrySpec, parentUrl);\n"
            "  const req = createRequire(profileDir + '/cordis.patch.yml');\n"
            "  const entryPath = req.resolve(entrySpec);\n"
            "  const mod = await import(pathToFileURL(entryPath).href);\n"
            "  console.log(JSON.stringify({\n"
            "    localNames: resolution.localPackageNames,\n"
            "    pkgDir: pkg?.dir,\n"
            "    entryPath,\n"
            "    capability: mod.ownerNoticesCapability,\n"
            "  }));\n"
            "} finally {\n"
            "  await ctx.fiber.dispose();\n"
            "}\n"
        )
        parity_proc = subprocess.run(
            ['node', '--import', 'tsx/esm', '--input-type=module', '-e', parity_script],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(parity_proc.returncode, 0, msg=f'parity stdout={parity_proc.stdout}\nstderr={parity_proc.stderr}')
        parity = json.loads(parity_proc.stdout.strip().splitlines()[-1])
        self.assertIn('@deepseek-ai/dsh-experimental-im-feishu', parity['localNames'])
        self.assertEqual(Path(parity['pkgDir']).resolve(), profile_pkg_copy.resolve())
        self.assertEqual(Path(parity['entryPath']).resolve(), Path(fp_info['entryPath']).resolve())
        self.assertTrue(parity['capability']['supported'])

        installed_policy = json.loads(policy_path.read_text(encoding='utf-8'))
        notif = installed_policy['wechatNotifications']
        self.assertTrue(notif['enabled'])
        self.assertFalse(notif['manualReplyEnabled'])
        self.assertEqual(notif['handoffDir'], str(handoff_dir))
        self.assertEqual(notif['handoffGid'], 22005)
        self.assertEqual(notif['receiptProducerUid'], (wechat_dir / 'receipts').stat().st_uid)
        self.assertEqual(notif['inboundProducerUid'], (wechat_dir / 'receipts').stat().st_uid)
        self.assertEqual((handoff_dir.stat().st_mode & 0o777), 0o750)
        self.assertEqual(((wechat_dir / 'adhoc/receipts').stat().st_mode & 0o777), 0o700)
        self.assertEqual(((wechat_dir / 'inbox/events').stat().st_mode & 0o777), 0o700)
        self.assertIn((str(handoff_dir), 0, 22005), chown_log)

        updated_ingress = ingress_profile.read_text(encoding='utf-8')
        self.assertIn('ownerNotices:', updated_ingress)
        self.assertIn(f'handoffDirectory: {json.dumps(str(handoff_dir))}', updated_ingress)
        self.assertIn('pollIntervalMs: 2500', updated_ingress)

        # 5. Re-install omitting --notifications-config preserves enabled state and succeeds while pinned local ingress stays compatible...
        summary_reinstall = installer.install(
            source_dir, runtime_root, Path(node_bin),
            wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
            state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
            ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
            unit_dir=unit_dir, handoff_dir=handoff_dir, handoff_gid=22005,
            check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
        )
        self.assertEqual(summary_reinstall['wechatNotifications'], {'enabled': True})

        # ...but if the pinned local ingress is swapped to an old incompatible copy, preserved-enabled re-install fails closed before systemctl stop.
        shutil.rmtree(profile_pkg_copy)
        write_old_runtime_feishu(profile_pkg_copy, with_built_entry=True)
        fake_cmd.stop_calls.clear()
        with self.assertRaises(RuntimeError) as preserve_fail:
            installer.install(
                source_dir, runtime_root, Path(node_bin),
                wechat_dir=wechat_dir, release_root=release_root, backup_root_base=backup_root,
                state_dir=state_dir, policy_path=policy_path, socket_path=socket_path,
                ingress_db=ingress_db, ingress_profile=ingress_profile, agent_profile=agent_profile,
                unit_dir=unit_dir, handoff_dir=handoff_dir, handoff_gid=22005,
                check_root=False, run_cmd=fake_cmd, chown_fn=lambda *a: None,
            )
        self.assertIn('ownerNoticesCapability', str(preserve_fail.exception))
        self.assertEqual(fake_cmd.stop_calls, [])


class InboundScanSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for sub in [
            'adhoc/outbox', 'adhoc/receipts', 'outbox', 'receipts',
            'config/contacts/friend-2', 'config/groups/g1', 'inbox/events', 'inbox/evidence',
        ]:
            (self.root / sub).mkdir(parents=True, exist_ok=True)
        (self.root / 'inbox').chmod(0o750)
        (self.root / 'inbox/events').chmod(0o700)
        (self.root / 'inbox/evidence').chmod(0o700)
        (self.root / 'receipts').chmod(0o700)
        for rel, body in [
            ('config/account.png', b'acc'),
            ('config/own-chat.png', b'own'),
            ('config/octopus-friend-label.png', b'lbl'),
            ('config/recipient.png', b'f1-s'),
            ('config/recipient-sidebar.png', b'f1-sb'),
            ('config/recipient-card.png', b'f1-c'),
            ('config/contacts/friend-2/recipient.png', b'f2-s'),
            ('config/contacts/friend-2/recipient-sidebar.png', b'f2-sb'),
            ('config/contacts/friend-2/recipient-card.png', b'f2-c'),
            ('config/groups/g1/group-search.png', b'g1-s'),
            ('config/groups/g1/member-1.png', b'g1-m1'),
            ('config/groups/g1/group-name.png', b'g1-name'),
        ]:
            (self.root / rel).write_bytes(body)
        self.config_path = self.root / 'config/config.json'
        self.contacts_path = self.root / 'config/octopus-contacts.json'
        self.manual_reply_path = self.root / 'config/octopus-manual-reply.json'
        self.config = {
            'enabled': True,
            'moments_enabled': True,
            'direct_enabled': True,
            'group_enabled': True,
            'account_id': 'own-id',
            'visibility': 'private',
            'account_anchor': '/home/wechat/.gamma-moments/config/account.png',
            'own_chat_anchor': '/home/wechat/.gamma-moments/config/own-chat.png',
            'recipient_id': 'wx-1',
            'recipient_query': '阿明',
            'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
            'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
            'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
        }
        self.sidecar = {
            'schema_version': 1,
            'account_sha256': worker.sha('own-id'),
            'contacts': [
                {
                    'id': 'friend-1', 'type': 'friend', 'alias': '好友一', 'display_name': '阿明',
                    'enabled': True, 'direct_enabled': True, 'legacy': True,
                    'recipient_id': 'wx-1', 'recipient_query': '阿明',
                    'recipient_anchor': '/home/wechat/.gamma-moments/config/recipient.png',
                    'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/recipient-sidebar.png',
                    'recipient_card_anchor': '/home/wechat/.gamma-moments/config/recipient-card.png',
                },
                {
                    'id': 'friend-2', 'type': 'friend', 'alias': '好友二', 'display_name': '阿强',
                    'enabled': True, 'direct_enabled': False,
                    'recipient_id': 'wx-2', 'recipient_query': '阿强',
                    'recipient_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient.png',
                    'recipient_sidebar_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-sidebar.png',
                    'recipient_card_anchor': '/home/wechat/.gamma-moments/config/contacts/friend-2/recipient-card.png',
                },
                {
                    'id': 'group-1', 'type': 'group', 'alias': '只读观察群', 'display_name': '策略观察群',
                    'enabled': True, 'group_enabled': False,
                    'group_id': 'wx-g1', 'group_name': '策略观察群', 'group_query': '策略观察群',
                    'group_anchor': '/home/wechat/.gamma-moments/config/groups/g1/group-search.png',
                    'group_member_anchors': ['/home/wechat/.gamma-moments/config/groups/g1/member-1.png'],
                    'group_name_anchor': '/home/wechat/.gamma-moments/config/groups/g1/group-name.png',
                    'group_name_read_mode': 'anchor',
                },
            ],
        }
        self.config_path.write_text(json.dumps(self.config), encoding='utf-8')
        self.config_path.chmod(0o640)
        self.contacts_path.write_text(json.dumps(self.sidecar), encoding='utf-8')
        self.contacts_path.chmod(0o640)
        self._write_manual_reply_view(enabled=True)
        Desktop.submissions = []
        Desktop.fail_after_submit = False

    def _write_manual_reply_view(self, *, enabled=True, manual_reply_enabled=False, paused=False, account_id='own-id', mode=0o640):
        self.manual_reply_path.write_text(json.dumps({
            'schema_version': 1,
            'account_sha256': worker.sha(account_id),
            'enabled': enabled,
            'manual_reply_enabled': manual_reply_enabled,
            'max_age_seconds': 3600,
            'paused': paused,
            'lease_expires_at': None,
        }), encoding='utf-8')
        self.manual_reply_path.chmod(mode)

    def _friend_job(self, job_id, text='主动发送测试', *, contact_index=0, created_ms=None):
        contact = self.sidecar['contacts'][contact_index]
        now_ms = int(time.time() * 1000) if created_ms is None else created_ms
        job = {
            'schema_version': 2,
            'kind': 'send',
            'job_id': job_id,
            'account_sha256': worker.sha('own-id'),
            'contact_id': contact['id'],
            'contact_type': 'friend',
            'recipient_id': contact['recipient_id'],
            'recipient_alias': contact['alias'],
            'binding_sha256': worker.compute_binding_sha256(self.root, self.config, contact),
            'text': text,
            'text_sha256': worker.sha(text),
            'created_at': now_ms,
            'expires_at': now_ms + 600_000,
            'confirmation_request_id': f'req-{job_id[:6]}',
        }
        p = self.root / 'adhoc/outbox' / f'{job_id}.json'
        p.write_text(json.dumps(job), encoding='utf-8')
        p.chmod(0o640)
        return p

    def _png_bytes(self, color=(18, 92, 168)):
        import io
        from PIL import Image
        buf = io.BytesIO()
        Image.new('RGB', (120, 48), color).save(buf, format='PNG')
        return buf.getvalue()

    def _make_calibrated_adapter(
        self,
        *,
        supports_unread_hint=False,
        hint_fn=None,
        scan_fn=None,
        restore_fn=None,
        identity_override=None,
        supported_kinds=('friend', 'group', 'own_post'),
        now_ms_fn=None,
    ):
        test_self = self
        from gamma_cn.moments.inbox import AccountBindingProof, InboundObservation

        class CalibratedFakeScanner:
            INBOUND_SCAN_CAPABILITY = {
                'schema_version': 1,
                'supported': True,
                'calibrated': True,
                'supported_target_kinds': supported_kinds,
                'supports_unread_hint': supports_unread_hint,
            }

            def __init__(inner_self):
                inner_self.events = []
                inner_self.scanned = []
                inner_self.restored = []
                inner_self.hints = []
                inner_self.seq = 0

            def verify_account_identity(inner_self, cfg):
                inner_self.events.append('identity')
                if callable(identity_override):
                    return identity_override(cfg)
                if identity_override is not None:
                    return identity_override
                return worker.sha(cfg['account_id'])

            def read_unread_hint(inner_self, *, account_sha256, targets, deadline_monotonic, config):
                inner_self.events.append('hint')
                inner_self.hints.append([t.target_key for t in targets])
                if hint_fn is not None:
                    return hint_fn(account_sha256, targets, deadline_monotonic, config)
                return worker.UnreadHintResult(
                    schema_version=1,
                    verified=True,
                    account_sha256=account_sha256,
                    has_unread=False,
                    observed_at_ms=now_ms_fn() if now_ms_fn else int(time.time() * 1000),
                )

            def scan_target(inner_self, target, *, deadline_monotonic, slice_budget_seconds, root, config):
                inner_self.seq += 1
                inner_self.events.append(f'scan:{target.target_key}')
                inner_self.scanned.append(target.target_key)
                # Confirm the shared account lock is already held by the enclosing worker slice.
                probe_q = Queue(Path(root) / 'outbox', Path(root) / 'receipts')
                try:
                    with probe_q.lock(target.account_sha256):
                        raise AssertionError('Expected shared account lock to be held during scan_target')
                except BlockingIOError:
                    pass
                if scan_fn is not None:
                    return scan_fn(target, deadline_monotonic, slice_budget_seconds, root, config, inner_self.seq)
                obs_ms = now_ms_fn() if now_ms_fn else int(time.time() * 1000)
                if target.kind == 'own_post':
                    obs = InboundObservation(
                        account_sha256=target.account_sha256,
                        trigger_type='moments_interaction',
                        conversation_kind='moments',
                        conversation_ref=target.target_id,
                        conversation_name='朋友圈评论',
                        sender_name='阿明',
                        sender_ref='wx-1',
                        source_ref=f'cmt-{inner_self.seq}',
                        source_time='19:01',
                        observed_at=obs_ms,
                        text='谢谢分享',
                        evidence_crop=test_self._png_bytes(),
                    )
                else:
                    obs = InboundObservation(
                        account_sha256=target.account_sha256,
                        trigger_type='friend_message' if target.kind == 'friend' else 'group_message',
                        conversation_kind=target.kind,
                        conversation_ref=target.target_id,
                        conversation_name=target.display_name,
                        sender_name='阿明',
                        source_ref=f'msg-{inner_self.seq}',
                        source_time='19:01',
                        observed_at=obs_ms,
                        text=f'来自{target.target_id}的入站消息#{inner_self.seq}',
                        evidence_crop=test_self._png_bytes(),
                        binding_proof=AccountBindingProof(
                            account_sha256=target.account_sha256,
                            conversation_kind=target.kind,
                            conversation_ref=target.target_id,
                            binding_sha256=target.binding_sha256,
                            lock_verified=True,
                        ),
                    )
                return worker.ScanSliceResult(
                    target_key=target.target_key,
                    observations=(obs,),
                    ui_restored=True,
                )

            def restore_ui(inner_self, target):
                inner_self.events.append(f'restore:{target.target_key}')
                inner_self.restored.append(target.target_key)
                if restore_fn is not None:
                    return restore_fn(target)
                return {'ui_restored': True, 'draft_present': False}

        return CalibratedFakeScanner()

    def test_default_off_and_uncalibrated_optional_adapter_preserve_existing_start(self):
        import importlib
        import types
        from unittest import mock

        # When Gamma provides a calibrated inbound scanner on PYTHONPATH, verify its advertised shape.
        probed_adapter, available = worker._probe_optional_inbound_scan_adapter()
        if available:
            self.assertIsNotNone(probed_adapter)
            advertised = worker.validate_inbound_scan_adapter(probed_adapter)
            self.assertEqual(advertised['schema_version'], worker.INBOUND_SCAN_SCHEMA_VERSION)
            self.assertTrue(advertised['supported'])
            self.assertTrue(advertised['calibrated'])
            self.assertIsInstance(advertised['supported_target_kinds'], frozenset)
            self.assertTrue(advertised['supported_target_kinds'].issubset(worker.INBOUND_SCAN_TARGET_KINDS))
            self.assertIn('friend', advertised['supported_target_kinds'])
            self.assertIs(type(advertised['supports_unread_hint']), bool)
            for method_name in ('verify_account_identity', 'scan_target', 'restore_ui'):
                self.assertTrue(callable(getattr(probed_adapter, method_name, None)))
        else:
            self.assertIsNone(probed_adapter)

        real_import_module = importlib.import_module

        def _missing_scanner_import(name, package=None):
            if name == 'gamma_cn.moments.inbound_scanner':
                raise ModuleNotFoundError("No module named 'gamma_cn.moments.inbound_scanner'")
            return real_import_module(name, package)

        with mock.patch.object(importlib, 'import_module', side_effect=_missing_scanner_import):
            self.assertEqual(worker._probe_optional_inbound_scan_adapter(), (None, False))

        class _UncalibratedImportedScanner:
            INBOUND_SCAN_CAPABILITY = {
                'schema_version': 1,
                'supported': True,
                'calibrated': False,
                'supported_target_kinds': frozenset({'friend'}),
            }

            def verify_account_identity(self, cfg):
                return ''

            def scan_target(self, target, **kw):
                return None

            def restore_ui(self, target):
                return True

        def _uncalibrated_scanner_import(name, package=None):
            if name == 'gamma_cn.moments.inbound_scanner':
                return types.SimpleNamespace(create_inbound_scanner=lambda: _UncalibratedImportedScanner())
            return real_import_module(name, package)

        with mock.patch.object(importlib, 'import_module', side_effect=_uncalibrated_scanner_import):
            self.assertEqual(worker._probe_optional_inbound_scan_adapter(), (None, False))

        from gamma_cn.moments.comments import UncalibratedCommentUiDriver
        for bad_adapter in (
            None,
            UncalibratedCommentUiDriver(),
            type('NoCap', (), {'verify_account_identity': lambda s, c: '', 'scan_target': lambda s, t, **kw: None, 'restore_ui': lambda s, t: True})(),
            type('Uncalibrated', (), {
                'INBOUND_SCAN_CAPABILITY': {'schema_version': 1, 'supported': True, 'calibrated': False, 'supported_target_kinds': ('friend',)},
                'verify_account_identity': lambda s, c: '',
                'scan_target': lambda s, t, **kw: None,
                'restore_ui': lambda s, t: True,
            })(),
            type('BadSchema', (), {
                'INBOUND_SCAN_CAPABILITY': {'schema_version': 99, 'supported': True, 'calibrated': True, 'supported_target_kinds': ('friend',)},
                'verify_account_identity': lambda s, c: '',
                'scan_target': lambda s, t, **kw: None,
                'restore_ui': lambda s, t: True,
            })(),
            type('MissingHintMethod', (), {
                'INBOUND_SCAN_CAPABILITY': {'schema_version': 1, 'supported': True, 'calibrated': True, 'supported_target_kinds': ('friend',), 'supports_unread_hint': True},
                'verify_account_identity': lambda s, c: '',
                'scan_target': lambda s, t, **kw: None,
                'restore_ui': lambda s, t: True,
            })(),
        ):
            with self.assertRaisesRegex(ValueError, 'gamma_inbound_scan_adapter_unavailable'):
                worker.validate_inbound_scan_adapter(bad_adapter)

        # Default worker start (no inbound scan config) runs identically irrespective of installed scanner capability.
        now_ms = int(time.time() * 1000)
        p_send = self._friend_job('a1' * 12, text='默认启动发送', created_ms=now_ms)
        daily_calls = []

        class LoggedInDesktop(Desktop):
            def logged_in(inner_self):
                return True

        def fake_tick(r, c):
            daily_calls.append((Path(r), Path(c)))
            return {'state': 'ready'}

        worker.main(
            [
                '--root', str(self.root),
                '--config', str(self.config_path),
                '--contacts-config', str(self.contacts_path),
                '--legacy-module', 'gamma_cn.moments.worker',
                '--poll-seconds', '2',
                '--once',
            ],
            trusted_uid=os.getuid(),
            tick_fn=fake_tick,
            desktop_class=LoggedInDesktop,
            manual_reply_path=self.manual_reply_path,
        )
        self.assertEqual(len(daily_calls), 1)
        self.assertEqual(Desktop.submissions, ['默认启动发送'])
        self.assertEqual(json.loads((self.root / 'adhoc/receipts' / p_send.name).read_text())['state'], 'sent_verified')
        self.assertFalse((self.root / 'receipts/worker.pid.json').exists())
        self.assertEqual(list((self.root / 'inbox/events').glob('*.json')), [])

        # Enabling inbound scan fails closed at startup when the import boundary yields no calibrated adapter.
        with mock.patch.object(importlib, 'import_module', side_effect=_missing_scanner_import):
            with self.assertRaisesRegex(RuntimeError, 'gamma_inbound_scan_adapter_unavailable'):
                worker.main(
                    [
                        '--root', str(self.root),
                        '--config', str(self.config_path),
                        '--contacts-config', str(self.contacts_path),
                        '--legacy-module', 'gamma_cn.moments.worker',
                        '--poll-seconds', '2',
                        '--once',
                    ],
                    inbound_scan_config={'enabled': True},
                    trusted_uid=os.getuid(),
                    tick_fn=fake_tick,
                    desktop_class=LoggedInDesktop,
                    manual_reply_path=self.manual_reply_path,
                )
        with mock.patch.object(importlib, 'import_module', side_effect=_uncalibrated_scanner_import):
            with self.assertRaisesRegex(RuntimeError, 'gamma_inbound_scan_adapter_unavailable'):
                worker.main(
                    [
                        '--root', str(self.root),
                        '--config', str(self.config_path),
                        '--contacts-config', str(self.contacts_path),
                        '--legacy-module', 'gamma_cn.moments.worker',
                        '--poll-seconds', '2',
                        '--once',
                    ],
                    inbound_scan_config={'enabled': True},
                    trusted_uid=os.getuid(),
                    tick_fn=fake_tick,
                    desktop_class=LoggedInDesktop,
                    manual_reply_path=self.manual_reply_path,
                )

    def test_installer_verifies_staged_worker_and_requires_real_scanner_only_when_enabled(self):
        source = Path(__file__).resolve().parents[1]
        disabled = installer.verify_gamma_inbound_scan(
            source, {'enabled': False}, sidecar_doc=self.sidecar,
            notifications=False, in_process=True,
        )
        self.assertEqual(disabled, {'enabled': False, 'supportedTargetKinds': []})
        with self.assertRaisesRegex(RuntimeError, 'requires enabled WeChat notifications'):
            installer.verify_gamma_inbound_scan(
                source, {'enabled': True}, sidecar_doc=self.sidecar,
                notifications=False, in_process=True,
            )
        with self.assertRaisesRegex(RuntimeError, 'self_mention_names'):
            installer.verify_gamma_inbound_scan(
                source, {'enabled': True}, sidecar_doc=self.sidecar,
                notifications=True, in_process=True,
            )

    def test_cli_scan_configuration_is_read_on_start_and_each_worker_turn(self):
        path = self.root / 'config/octopus-inbound-scan.json'
        path.write_text(json.dumps({'enabled': True}), encoding='utf-8')
        path.chmod(0o640)

        class ImmediateStop:
            def is_set(self):
                return False

            def wait(self, timeout):
                path.write_text(json.dumps({'enabled': False}), encoding='utf-8')

        scanner = self._make_calibrated_adapter(supported_kinds=('friend',))
        args = [
            '--root', str(self.root),
            '--config', str(self.config_path),
            '--contacts-config', str(self.contacts_path),
            '--inbound-scan-config', str(path),
            '--legacy-module', 'gamma_cn.moments.worker',
            '--poll-seconds', '2',
        ]
        worker.main(
            args, stopping=ImmediateStop(), max_iterations=2, trusted_uid=os.getuid(),
            tick_fn=lambda root, config: {}, desktop_class=Desktop,
            inbound_scan_adapter=scanner, manual_reply_path=self.manual_reply_path,
        )
        self.assertEqual(scanner.scanned, ['friend:friend-1'])
        self.assertEqual(len(list((self.root / 'inbox/events').glob('*.json'))), 1)

        protected = path.with_name('untrusted.json')
        protected.write_text('{"enabled":false}', encoding='utf-8')
        protected.chmod(0o644)
        path.unlink()
        path.symlink_to(protected)
        with self.assertRaisesRegex(ValueError, 'untrusted_inbound_scan_config'):
            worker.main(
                args, max_iterations=1, trusted_uid=os.getuid(),
                tick_fn=lambda root, config: {}, desktop_class=Desktop,
                inbound_scan_adapter=scanner, manual_reply_path=self.manual_reply_path,
            )

    def test_auto_reply_requires_current_permit_and_native_tail_under_shared_lock(self):
        from types import SimpleNamespace
        from unittest.mock import patch

        now = int(time.time() * 1000)
        permit_path = self.root / 'config/octopus-auto-reply.json'
        permit = {
            'schema_version': 1, 'account_sha256': worker.sha('own-id'), 'policy_version': 7,
            'enabled': True, 'paused': False, 'lease_expires_at': now + 10000,
            'conversations': {'friend:friend-1': 'unattended'},
        }
        permit_path.write_text(json.dumps(permit), encoding='utf-8')
        permit_path.chmod(0o640)
        source = {
            'source_ref': 'msg-fixture', 'text_sha256': worker.sha('trigger'),
            'evidence_sha256': worker.sha(self._png_bytes()), 'observed_at': now - 1000,
        }

        def auto_job(task_id):
            path = self._friend_job(task_id, text='自动回复', created_ms=now)
            job = json.loads(path.read_text())
            job.update(
                auto_reply_task_id=task_id, auto_reply_policy_version=7,
                auto_reply_source=source, confirmation_request_id='wechat-auto:' + task_id,
            )
            path.write_text(json.dumps(job), encoding='utf-8')
            return path

        def run_automatic(task_id, verify=None):
            path = auto_job(task_id)
            module = SimpleNamespace(
                verify_reply_tail=verify,
                REPLY_TAIL_TARGET_KINDS=frozenset({'friend'}),
            )
            with patch.object(worker.importlib, 'import_module', return_value=module):
                return worker.process_job(
                    path, self.root, self.config, Queue, run_one, Desktop,
                    clock=lambda: now / 1000, trusted_uid=os.getuid(),
                    config_path=self.config_path, contacts_path=self.contacts_path,
                    manual_reply_path=self.manual_reply_path,
                )

        seen = []

        def check_tail(target, *, root, source, desktop, deadline_monotonic):
            self.assertEqual(target.target_key, 'friend:friend-1')
            self.assertEqual(source['source_ref'], 'msg-fixture')
            self.assertEqual(desktop.text, '自动回复')
            with self.assertRaises(BlockingIOError):
                with Queue(root / 'outbox', root / 'receipts').lock(target.account_sha256):
                    self.fail('preflight performed outside the shared lock')
            seen.append(target.target_id)
            return True

        unsupported = run_automatic('a1' * 12)
        self.assertEqual(unsupported['reason'], 'auto_reply_tail_adapter_unavailable')
        changed = run_automatic('a2' * 12, lambda *a, **kw: False)
        self.assertEqual(changed['state'], 'failed_before_submit')
        self.assertEqual(changed['reason'], 'auto_reply_tail_not_verified')
        self.assertEqual(Desktop.submissions, [])
        accepted = run_automatic('a3' * 12, check_tail)
        self.assertEqual(accepted['state'], 'sent_verified')
        self.assertEqual(seen, ['friend-1'])

        def pause_during_tail(target, **kwargs):
            # The auto permit is deliberately still current. Manual pause wins.
            self._write_manual_reply_view(paused=True)
            return True

        paused = run_automatic('a4' * 12, pause_during_tail)
        self.assertEqual(paused['reason'], 'auto_reply_paused')
        self.assertEqual(Desktop.submissions, ['自动回复'])

    def test_root_owned_config_resolution_malformed_default_off_and_incompatible_bounds(self):
        # Malformed or out-of-bounds InboundScanConfig fails closed.
        self.assertFalse(worker.InboundScanConfig().enabled)
        for bad_cfg in (
            {'unknown_key': True},
            {'schema_version': 2, 'enabled': True},
            {'enabled': 'yes'},
            {'enabled': True, 'slice_budget_seconds': True},
            {'enabled': True, 'slice_budget_seconds': 0.1},
            {'enabled': True, 'idle_scan_interval_seconds': 20.0, 'fallback_interval_seconds': 10.0},
            {'enabled': True, 'fairness_max_deferral_seconds': 1.0},
            {'enabled': True, 'fairness_max_outbound_ticks': 0},
            {'enabled': True, 'contact_ids': ['friend-1', 'friend-1']},
            {'enabled': True, 'registered_own_posts': ['*']},
            {'enabled': True, 'registered_own_posts': ['post-1', 'post-1']},
            {'enabled': True, 'registered_own_posts': [{'own_post_id': 'post-1', 'extra': 1}]},
        ):
            with self.assertRaises(ValueError):
                worker.InboundScanConfig.from_mapping(bad_cfg)

        scan_cfg = worker.InboundScanConfig(
            enabled=True,
            registered_own_posts=(worker.RegisteredOwnPostTarget(own_post_id='own_post_20261121_01', post_label='复盘帖'),),
        )
        # Valid root-owned config resolves bound friends (even direct_enabled=False), bound group (even group_enabled=False), and own_post.
        resolved = worker.resolve_inbound_scan_targets(
            self.root,
            self.config,
            trusted_uid=os.getuid(),
            config_path=self.config_path,
            contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path,
            scan_config=scan_cfg,
        )
        self.assertEqual(resolved['state'], 'ready')
        self.assertEqual(
            [t.target_key for t in resolved['targets']],
            ['friend:friend-1', 'friend:friend-2', 'group:group-1', 'own_post:own_post_20261121_01'],
        )
        self.assertEqual(
            resolved['targets'][0].binding_sha256,
            worker.compute_binding_sha256(self.root, self.config, self.sidecar['contacts'][0]),
        )
        self.assertEqual(
            resolved['targets'][2].binding_sha256,
            worker.compute_group_binding_sha256(self.root, self.config, self.sidecar['contacts'][2]),
        )
        scoped = worker.InboundScanConfig(
            enabled=True, contact_ids=('group-1',), self_mention_names=('Bot',),
            group_inbound={'group-1': {'sender_verify_mode': 'pinned_only'}},
        )
        group = worker.resolve_inbound_scan_targets(
            self.root, self.config, trusted_uid=os.getuid(), config_path=self.config_path,
            contacts_path=self.contacts_path, manual_reply_path=self.manual_reply_path, scan_config=scoped,
        )['targets']
        self.assertEqual([target.target_key for target in group], ['group:group-1'])
        self.assertEqual(group[0].effective_config['sender_verify_mode'], 'pinned_only')

        # Disabled worker config or disabled/missing manual_reply view keeps scan disabled cleanly.
        self.assertEqual(
            worker.resolve_inbound_scan_targets(
                self.root, {**self.config, 'enabled': False}, trusted_uid=os.getuid(),
                contacts_path=self.contacts_path, manual_reply_path=self.manual_reply_path, scan_config=scan_cfg,
            )['state'],
            'disabled',
        )
        self._write_manual_reply_view(enabled=False)
        self.assertEqual(
            worker.resolve_inbound_scan_targets(
                self.root, self.config, trusted_uid=os.getuid(),
                contacts_path=self.contacts_path, manual_reply_path=self.manual_reply_path, scan_config=scan_cfg,
            )['reason'],
            'notifications_disabled',
        )
        self.manual_reply_path.unlink()
        self.assertEqual(
            worker.resolve_inbound_scan_targets(
                self.root, self.config, trusted_uid=os.getuid(),
                contacts_path=self.contacts_path, manual_reply_path=self.manual_reply_path, scan_config=scan_cfg,
            )['reason'],
            'notifications_disabled',
        )

        # Missing sidecar never infers targets from legacy config.json.
        self._write_manual_reply_view(enabled=True)
        self.contacts_path.unlink()
        no_sidecar = worker.resolve_inbound_scan_targets(
            self.root, self.config, trusted_uid=os.getuid(),
            config_path=self.config_path, manual_reply_path=self.manual_reply_path,
            scan_config=worker.InboundScanConfig(enabled=True),
        )
        self.assertEqual(no_sidecar['state'], 'disabled')
        self.assertEqual(no_sidecar['reason'], 'contacts_registry_missing')

        # Malformed, writable, or account-mismatched root-owned files fail closed during worker iteration without breaking daily tick or outbox.
        self.contacts_path.write_text('{corrupt', encoding='utf-8')
        self.contacts_path.chmod(0o640)
        adapter = self._make_calibrated_adapter()
        scheduler = worker.InboundScanScheduler(scan_cfg)
        daily_calls = []
        turn_corrupt = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: daily_calls.append(1) or {'state': 'ready'},
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=adapter,
        )
        self.assertEqual(len(daily_calls), 1)
        self.assertEqual(turn_corrupt['scan']['state'], 'refused')
        self.assertEqual(adapter.scanned, [])

        # Restore sidecar, make manual_reply view world-writable -> refused without calling adapter, while normal outbox still runs!
        self.contacts_path.write_text(json.dumps(self.sidecar), encoding='utf-8')
        self.contacts_path.chmod(0o640)
        self._write_manual_reply_view(enabled=True, mode=0o666)
        p_outbox = self._friend_job('a2' * 12, text='视图异常时普通主动发送仍有效')
        turn_bad_view = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: {'state': 'ready'},
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=adapter,
        )
        self.assertEqual(turn_bad_view['outbox'][0]['state'], 'sent_verified')
        self.assertEqual(turn_bad_view['scan']['state'], 'refused')
        self.assertEqual(turn_bad_view['scan']['reason'], 'untrusted_manual_reply_config')
        self.assertEqual(adapter.scanned, [])
        self.assertEqual(json.loads((self.root / 'adhoc/receipts' / p_outbox.name).read_text())['state'], 'sent_verified')

        # Symlinked anchor file fails closed instead of silently skipping or guessing.
        self._write_manual_reply_view(enabled=True, mode=0o640)
        card = self.root / 'config/recipient-card.png'
        card.unlink()
        card.symlink_to(self.root / 'config/account.png')
        turn_sym = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=adapter,
        )
        self.assertEqual(turn_sym['scan']['state'], 'refused')
        self.assertEqual(adapter.scanned, [])

    def test_real_worker_turn_flow_single_target_slice_rotation_and_no_lock_loops(self):
        wall_ms = [1_760_000_000_000]
        mono_s = [100.0]
        adapter = self._make_calibrated_adapter(now_ms_fn=lambda: wall_ms[0])
        scan_cfg = worker.InboundScanConfig(
            enabled=True,
            slice_budget_seconds=5.0,
            idle_scan_interval_seconds=5.0,
            fallback_interval_seconds=5.0,
            fairness_max_deferral_seconds=20.0,
            use_unread_hint=False,
            registered_own_posts=('own_post_20261121_01',),
        )
        daily_calls = []

        def fake_tick(r, c):
            daily_calls.append(mono_s[0])
            return {'state': 'ready'}

        # Drive 5 full turns through worker.main() with a deterministic clock advancing 5s per iteration.
        class AdvancingStop:
            def __init__(inner_self):
                inner_self.count = 0
                inner_self._set = False

            def is_set(inner_self):
                return inner_self._set

            def set(inner_self):
                inner_self._set = True

            def wait(inner_self, seconds):
                inner_self.count += 1
                mono_s[0] += 5.0
                wall_ms[0] += 5000
                return False

        worker.main(
            [
                '--root', str(self.root),
                '--config', str(self.config_path),
                '--contacts-config', str(self.contacts_path),
                '--legacy-module', 'gamma_cn.moments.worker',
                '--poll-seconds', '2',
            ],
            stopping=AdvancingStop(),
            max_iterations=5,
            inbound_scan_config=scan_cfg,
            inbound_scan_adapter=adapter,
            trusted_uid=os.getuid(),
            clock=lambda: wall_ms[0] / 1000.0,
            monotonic_clock=lambda: mono_s[0],
            tick_fn=fake_tick,
            desktop_class=Desktop,
            manual_reply_path=self.manual_reply_path,
        )
        self.assertEqual(
            adapter.scanned,
            [
                'friend:friend-1',
                'friend:friend-2',
                'group:group-1',
                'own_post:own_post_20261121_01',
                'friend:friend-1',
            ],
        )
        self.assertEqual(adapter.restored, adapter.scanned)
        # Exactly one identity -> scan -> restore sequence per lock tenure.
        expected_events = []
        for tk in adapter.scanned:
            expected_events.extend(['identity', f'scan:{tk}', f'restore:{tk}'])
        self.assertEqual(adapter.events, expected_events)
        # 4 distinct Wire Schema v1 events written (5th iteration hits same source_ref? No, seq=5 makes 5 events).
        events = sorted((self.root / 'inbox/events').glob('*.json'))
        self.assertEqual(len(events), 5)
        first_payload = json.loads(events[0].read_text(encoding='utf-8'))
        self.assertEqual(first_payload['schema_version'], 1)
        self.assertTrue((self.root / first_payload['evidence_path']).is_file())
        self.assertEqual(daily_calls, [100.0])

    def test_unread_hint_pre_screen_low_frequency_fallback_and_unverified_hint_rejection(self):
        wall_ms = [1_760_000_000_000]
        mono_s = [200.0]
        hint_state = {
            'has_unread': False,
            'verified': True,
            'account_override': None,
            'target_keys': (),
            'observed_at_ms': None,
        }

        def hint_fn(account_sha, targets, deadline, cfg):
            return worker.UnreadHintResult(
                schema_version=1,
                verified=hint_state['verified'],
                account_sha256=hint_state['account_override'] or account_sha,
                has_unread=hint_state['has_unread'],
                target_keys=hint_state['target_keys'],
                observed_at_ms=hint_state['observed_at_ms'] if hint_state['observed_at_ms'] is not None else wall_ms[0],
            )

        adapter = self._make_calibrated_adapter(
            supports_unread_hint=True,
            hint_fn=hint_fn,
            now_ms_fn=lambda: wall_ms[0],
        )
        scheduler = worker.InboundScanScheduler(worker.InboundScanConfig(
            enabled=True,
            slice_budget_seconds=5.0,
            idle_scan_interval_seconds=5.0,
            fallback_interval_seconds=30.0,
            fairness_max_deferral_seconds=20.0,
            use_unread_hint=True,
            registered_own_posts=('own_post_20261121_01',),
        ))

        def step(advance_s):
            mono_s[0] += advance_s
            wall_ms[0] += int(advance_s * 1000)
            return worker.run_worker_iteration(
                self.root, self.config_path,
                scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
                tick_fn=lambda r, c: {'state': 'ready'},
                clock=lambda: wall_ms[0] / 1000.0,
                monotonic_clock=lambda: mono_s[0],
                trusted_uid=os.getuid(),
                contacts_path=self.contacts_path,
                manual_reply_path=self.manual_reply_path,
                inbound_scan_adapter=adapter,
            )

        # t=200: initial scan (fallback_due=True since last_scan_mono is None) -> friend-1.
        r0 = step(0)
        self.assertEqual(r0['scan']['state'], 'scanned')
        self.assertEqual(r0['scan']['target_key'], 'friend:friend-1')

        # t=202: inside 5s cooldown -> lock not touched at all.
        events_before = len(adapter.events)
        r1 = step(2)
        self.assertEqual(r1['scan']['state'], 'cooldown')
        self.assertEqual(len(adapter.events), events_before)

        # t=206: >=5s, <30s, hint says no unread -> idle_hint_clear without opening any target.
        r2 = step(4)
        self.assertEqual(r2['scan']['state'], 'idle_hint_clear')
        self.assertEqual(len(adapter.scanned), 1)

        # t=212: hint says unread on group-1 AND own_post -> scans ONLY group-1 (1 target per slice).
        hint_state['has_unread'] = True
        hint_state['target_keys'] = ('group:group-1', 'own_post:own_post_20261121_01')
        r3 = step(6)
        self.assertEqual(r3['scan']['state'], 'scanned')
        self.assertEqual(r3['scan']['target_key'], 'group:group-1')

        # t=218: hint still lists both -> fair rotation moves to own_post.
        r4 = step(6)
        self.assertEqual(r4['scan']['state'], 'scanned')
        self.assertEqual(r4['scan']['target_key'], 'own_post:own_post_20261121_01')

        # Unverified or unregistered hint targets fail closed without scanning.
        hint_state['target_keys'] = ('friend:unregistered-stranger',)
        r_bad_target = step(6)
        self.assertEqual(r_bad_target['scan']['state'], 'refused')
        self.assertEqual(r_bad_target['scan']['reason'], 'unregistered_unread_hint_target')

        hint_state['target_keys'] = ()
        hint_state['verified'] = False
        r_unverified = step(6)
        self.assertEqual(r_unverified['scan']['state'], 'refused')
        self.assertEqual(r_unverified['scan']['reason'], 'unverified_unread_hint')

        # t=250 (>=30s since last scan at 218): fallback fires even when hint says has_unread=False!
        hint_state['verified'] = True
        hint_state['has_unread'] = False
        r_fallback = step(20)
        self.assertEqual(r_fallback['scan']['state'], 'scanned')
        self.assertTrue(r_fallback['scan']['fallback'])
        self.assertEqual(r_fallback['scan']['target_key'], 'friend:friend-1')

    def test_outbox_once_per_pending_item_bounded_fairness_and_new_outbox_after_scan(self):
        wall_ms = [1_760_000_000_000]
        mono_s = [500.0]
        adapter = self._make_calibrated_adapter(now_ms_fn=lambda: wall_ms[0])
        scheduler = worker.InboundScanScheduler(worker.InboundScanConfig(
            enabled=True,
            slice_budget_seconds=5.0,
            idle_scan_interval_seconds=5.0,
            fallback_interval_seconds=60.0,
            fairness_max_deferral_seconds=10.0,
            use_unread_hint=False,
        ))

        # Already-terminal job is NOT re-run; pending jobs are attempted ONCE each per tick.
        done_id = 'd0' * 12
        self._friend_job(done_id, text='已终态任务', created_ms=wall_ms[0])
        worker.save(self.root / 'adhoc/receipts' / f'{done_id}.json', {'state': 'sent_verified'})

        attempt_counts = {}
        orig_run_one = run_one

        def counting_run_one(queue, key, desktop, **kw):
            attempt_counts[key] = attempt_counts.get(key, 0) + 1
            return orig_run_one(queue, key, desktop, **kw)

        j1 = 'b1' * 12
        self._friend_job(j1, text='连续出站-1', created_ms=wall_ms[0])
        t0 = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=counting_run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: {'state': 'ready'},
            clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=adapter,
        )
        self.assertEqual(attempt_counts, {j1: 1})
        self.assertEqual(t0['scan']['state'], 'deferred')

        # Continuous outbound traffic at t+4s and t+8s keeps deferring scan; at t+10s fairness runs 1 scan slice after outbox.
        for idx, advance_s, expected_scan in [(2, 4.0, 'deferred'), (3, 4.0, 'deferred'), (4, 2.0, 'scanned')]:
            mono_s[0] += advance_s
            wall_ms[0] += int(advance_s * 1000)
            jid = f'b{idx}' * 12
            self._friend_job(jid, text=f'连续出站-{idx}', created_ms=wall_ms[0])
            turn = worker.run_worker_iteration(
                self.root, self.config_path,
                scheduler=scheduler, queue_class=Queue, run_one_fn=counting_run_one, desktop_class=Desktop,
                tick_fn=lambda r, c: {'state': 'ready'},
                clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
                trusted_uid=os.getuid(), contacts_path=self.contacts_path,
                manual_reply_path=self.manual_reply_path, inbound_scan_adapter=adapter,
            )
            self.assertEqual(attempt_counts[jid], 1)
            self.assertEqual(turn['scan']['state'], expected_scan)
        self.assertEqual(adapter.scanned, ['friend:friend-1'])

        # New outbox item arriving during a scan slice wakes main() immediately without sleeping poll_seconds.
        Desktop.submissions = []
        new_job_id = 'c9' * 12
        waits = []
        order = []

        class FastStop:
            def __init__(inner_self):
                inner_self.calls = 0

            def is_set(inner_self):
                return False

            def wait(inner_self, seconds):
                waits.append(seconds)
                inner_self.calls += 1
                return False

        def scan_then_drop_outbox(target, deadline_monotonic, slice_budget_seconds, root, cfg, seq):
            order.append(f'scan:{target.target_key}')
            if seq == 1:
                self._friend_job(new_job_id, text='扫描后立即插入的新出站', created_ms=wall_ms[0])
            return worker.ScanSliceResult(target_key=target.target_key, ui_restored=True)

        class TrackingDesktop(Desktop):
            def submit(inner_self):
                order.append(f'send:{inner_self.text}')
                return super().submit()

        adapter2 = self._make_calibrated_adapter(scan_fn=scan_then_drop_outbox, now_ms_fn=lambda: wall_ms[0])
        mono_s[0] += 12.0
        wall_ms[0] += 12000
        worker.main(
            [
                '--root', str(self.root),
                '--config', str(self.config_path),
                '--contacts-config', str(self.contacts_path),
                '--legacy-module', 'gamma_cn.moments.worker',
                '--poll-seconds', '2',
            ],
            stopping=FastStop(),
            max_iterations=2,
            inbound_scan_config=worker.InboundScanConfig(
                enabled=True, idle_scan_interval_seconds=5.0, fallback_interval_seconds=10.0, use_unread_hint=False,
            ),
            inbound_scan_adapter=adapter2,
            trusted_uid=os.getuid(),
            clock=lambda: wall_ms[0] / 1000.0,
            monotonic_clock=lambda: mono_s[0],
            tick_fn=lambda r, c: {'state': 'ready'},
            desktop_class=TrackingDesktop,
            manual_reply_path=self.manual_reply_path,
        )
        # Iteration 1 scanned friend-1, dropped new_job_id, skipped wait() -> Iteration 2 ran new_job_id immediately before any second scan!
        self.assertEqual(waits, [])
        self.assertEqual(order, ['scan:friend:friend-1', 'send:扫描后立即插入的新出站'])
        self.assertEqual(json.loads((self.root / 'adhoc/receipts' / f'{new_job_id}.json').read_text())['state'], 'sent_verified')

    def test_shared_account_lock_contention_with_gamma_queue_and_failure_recovery(self):
        wall_ms = [1_760_000_000_000]
        mono_s = [900.0]
        adapter = self._make_calibrated_adapter(now_ms_fn=lambda: wall_ms[0])
        scheduler = worker.InboundScanScheduler(worker.InboundScanConfig(
            enabled=True,
            idle_scan_interval_seconds=5.0,
            fallback_interval_seconds=10.0,
            use_unread_hint=False,
        ))

        gamma_q = Queue(self.root / 'outbox', self.root / 'receipts')
        pending_job = self._friend_job('e1' * 12, text='锁竞争后恢复发送', created_ms=wall_ms[0])
        with gamma_q.lock(worker.sha('own-id')):
            busy_turn = worker.run_worker_iteration(
                self.root, self.config_path,
                scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
                tick_fn=lambda r, c: {'state': 'ready'},
                clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
                trusted_uid=os.getuid(), contacts_path=self.contacts_path,
                manual_reply_path=self.manual_reply_path, inbound_scan_adapter=adapter,
            )
            self.assertEqual(busy_turn['outbox'], [{'job_id': 'e1' * 12, 'state': 'busy', 'reason': None}])
            self.assertEqual(busy_turn['scan']['state'], 'deferred')

            # Even when idle, holding Gamma account lock returns scan busy without invoking adapter or advancing cursor.
            idle_sched = worker.InboundScanScheduler(scheduler.scan_config)
            busy_scan = idle_sched.run_scan_slice(
                self.root, self.config, queue_class=Queue, adapter=adapter,
                clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
                trusted_uid=os.getuid(), config_path=self.config_path,
                contacts_path=self.contacts_path, manual_reply_path=self.manual_reply_path,
            )
            self.assertEqual(busy_scan['state'], 'busy')
            self.assertEqual(adapter.scanned, [])
            self.assertIsNone(idle_sched.last_target_key)

        # Once Gamma releases lock, pending outbox finishes cleanly and next idle slice starts on un-scanned friend-1.
        turn_clear = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: {'state': 'ready'},
            clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=adapter,
        )
        self.assertEqual(turn_clear['outbox'][0]['state'], 'sent_verified')
        self.assertEqual(json.loads((self.root / 'adhoc/receipts' / pending_job.name).read_text())['state'], 'sent_verified')

        # Failure recovery 1: scan_target raises -> restore_ui STILL runs, lock STILL releases, next turn recovers cleanly!
        def boom_scan(target, deadline_monotonic, slice_budget_seconds, root, cfg, seq):
            if target.target_key == 'friend:friend-1':
                raise RuntimeError('transient_ui_blip')
            return worker.ScanSliceResult(target_key=target.target_key, ui_restored=True)

        boom_adapter = self._make_calibrated_adapter(scan_fn=boom_scan, now_ms_fn=lambda: wall_ms[0])
        mono_s[0] += 15.0
        wall_ms[0] += 15000
        turn_fail = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: {'state': 'ready'},
            clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=boom_adapter,
        )
        self.assertEqual(turn_fail['scan']['state'], 'failed')
        self.assertEqual(boom_adapter.restored, ['friend:friend-1'])
        # Confirm account lock was released!
        with gamma_q.lock(worker.sha('own-id')):
            pass
        # Next eligible slice moves on to friend-2 cleanly.
        mono_s[0] += 6.0
        wall_ms[0] += 6000
        turn_recover = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: {'state': 'ready'},
            clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=boom_adapter,
        )
        self.assertEqual(turn_recover['scan']['state'], 'scanned')
        self.assertEqual(turn_recover['scan']['target_key'], 'friend:friend-2')

        # Failure recovery 2: pre-existing composer draft or restore failure records needs_review without clearing draft or wedging lock.
        draft_adapter = self._make_calibrated_adapter(
            scan_fn=lambda target, *a: worker.ScanSliceResult(
                target_key=target.target_key, ui_restored=False, draft_present=True, needs_review=True,
            ),
            restore_fn=lambda target: {'ui_restored': False, 'draft_present': True},
            now_ms_fn=lambda: wall_ms[0],
        )
        mono_s[0] += 11.0
        wall_ms[0] += 11000
        turn_draft = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: {'state': 'ready'},
            clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=draft_adapter,
        )
        self.assertEqual(turn_draft['scan']['state'], 'needs_review')
        self.assertEqual(turn_draft['scan']['reason'], 'existing_draft_requires_review')
        self.assertTrue(turn_draft['scan']['draft_present'])

        # Failure recovery 3: wrong account identity fails closed before scan_target, and cross-target observation is rejected!
        wrong_acct_adapter = self._make_calibrated_adapter(identity_override=worker.sha('other-account'))
        mono_s[0] += 11.0
        wall_ms[0] += 11000
        turn_auth = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: {'state': 'ready'},
            clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=wrong_acct_adapter,
        )
        self.assertEqual(turn_auth['scan']['state'], 'auth_required')
        self.assertEqual(wrong_acct_adapter.scanned, [])

        from gamma_cn.moments.inbox import AccountBindingProof, InboundObservation

        def cross_target_scan(target, deadline_monotonic, slice_budget_seconds, root, cfg, seq):
            wrong_target = 'friend-2' if target.target_id != 'friend-2' else 'friend-1'
            obs = InboundObservation(
                account_sha256=target.account_sha256,
                trigger_type='friend_message',
                conversation_kind='friend',
                conversation_ref=wrong_target,
                sender_name='越界目标',
                source_ref='bad-cross-1',
                observed_at=wall_ms[0],
                text='不应写入',
                evidence_crop=self._png_bytes(),
                binding_proof=AccountBindingProof(
                    account_sha256=target.account_sha256,
                    conversation_kind='friend',
                    conversation_ref=wrong_target,
                    binding_sha256=worker.sha('wrong'),
                    lock_verified=True,
                ),
            )
            return worker.ScanSliceResult(target_key=target.target_key, observations=(obs,), ui_restored=True)

        cross_adapter = self._make_calibrated_adapter(scan_fn=cross_target_scan, now_ms_fn=lambda: wall_ms[0])
        mono_s[0] += 11.0
        wall_ms[0] += 11000
        events_count_before = len(list((self.root / 'inbox/events').glob('*.json')))
        turn_cross = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: {'state': 'ready'},
            clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=cross_adapter,
        )
        self.assertEqual(turn_cross['scan']['state'], 'failed')
        self.assertEqual(turn_cross['scan']['reason'], 'observation_target_mismatch')
        self.assertEqual(len(cross_adapter.restored), 1)
        self.assertEqual(len(list((self.root / 'inbox/events').glob('*.json'))), events_count_before)

        # Failure recovery 4: non-preemptible UI overrun (e.g. ~30.4s navigation) restores UI, reports overrun, and cools down from release time.
        def slow_scan(target, deadline_monotonic, slice_budget_seconds, root, cfg, seq):
            mono_s[0] += 30.4
            wall_ms[0] += 30400
            return worker.ScanSliceResult(target_key=target.target_key, ui_restored=True)

        slow_adapter = self._make_calibrated_adapter(scan_fn=slow_scan, now_ms_fn=lambda: wall_ms[0])
        mono_s[0] += 11.0
        wall_ms[0] += 11000
        turn_slow = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: {'state': 'ready'},
            clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=slow_adapter,
        )
        self.assertEqual(turn_slow['scan']['state'], 'scanned')
        self.assertTrue(turn_slow['scan']['budget_exceeded'])
        self.assertAlmostEqual(turn_slow['scan']['elapsed_seconds'], 30.4, places=6)
        self.assertEqual(len(slow_adapter.restored), 1)
        with gamma_q.lock(worker.sha('own-id')):
            pass
        # Cooldown is measured from release time, so +1.0s does NOT immediately re-enter slow UI navigation.
        mono_s[0] += 1.0
        wall_ms[0] += 1000
        turn_cooldown = worker.run_worker_iteration(
            self.root, self.config_path,
            scheduler=scheduler, queue_class=Queue, run_one_fn=run_one, desktop_class=Desktop,
            tick_fn=lambda r, c: {'state': 'ready'},
            clock=lambda: wall_ms[0] / 1000.0, monotonic_clock=lambda: mono_s[0],
            trusted_uid=os.getuid(), contacts_path=self.contacts_path,
            manual_reply_path=self.manual_reply_path, inbound_scan_adapter=slow_adapter,
        )
        self.assertEqual(turn_cooldown['scan']['state'], 'cooldown')


if __name__ == '__main__':
    unittest.main()
