"""Single desktop owner: retain Gamma daily jobs and consume separately authorized ad-hoc jobs."""
import argparse
from dataclasses import dataclass
from typing import Optional, Tuple, Union
from datetime import datetime
import hashlib
import importlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import sys
import tempfile
import threading
import time
from zoneinfo import ZoneInfo

CONTAINER_MOMENTS_PREFIX = '/home/wechat/.gamma-moments/'
ANCHOR_ROLES = ('recipient_anchor', 'recipient_sidebar_anchor', 'recipient_card_anchor')
GROUP_NAME_READ_MODES = frozenset({'auto', 'native_copy', 'anchor'})
CONTACT_ID_RE = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$')
ADHOC_JOB_ID_RE = re.compile(r'^[0-9a-f]{24}$')
ORIGIN_NOTICE_ID_RE = re.compile(r'^wn_[0-9a-f]{12}$')
NOTICE_ORIGIN_KEY_RE = re.compile(r'^inbound:[a-zA-Z0-9._:-]{1,120}$')
MOMENTS_REF_RE = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$')
TERMINAL_RECEIPT_STATES = frozenset({
    'sent_verified',
    'published_verified',
    'identity_verified',
    'needs_review',
    'failed_before_submit',
    'refused',
    'expired',
    'auth_required',
})
INBOUND_SCAN_SCHEMA_VERSION = 1
INBOUND_SCAN_TARGET_KINDS = frozenset({'friend', 'group', 'own_post'})
INBOUND_SCAN_BOUNDS = {
    'slice_budget_seconds': (0.5, 55.0),
    'idle_scan_interval_seconds': (2.0, 300.0),
    'fallback_interval_seconds': (2.0, 3600.0),
    'fairness_max_deferral_seconds': (5.0, 600.0),
    'fairness_max_outbound_ticks': (1, 100),
}
MAX_REGISTERED_OWN_POSTS = 20
MAX_WORKER_CONFIG_BYTES = 64 * 1024
MIN_OBSERVED_MS = 1_700_000_000_000
MAX_OBSERVED_MS = 4_102_444_800_000
HINT_FUTURE_SKEW_MS = 60_000
HINT_MAX_AGE_MS = 60_000
NOTICE_REPLY_FIELDS = frozenset({
    'origin_notice_id',
    'origin_notice_key',
    'origin_notice_observed_at',
    'origin_notice_created_at',
})
AUTO_REPLY_FIELDS = frozenset({'auto_reply_task_id', 'auto_reply_policy_version', 'auto_reply_source'})
AUTO_REPLY_SOURCE_KEYS = frozenset({'source_ref', 'text_sha256', 'evidence_sha256', 'observed_at'})
AUTO_REPLY_VIEW_KEYS = frozenset({
    'schema_version', 'account_sha256', 'policy_version', 'enabled', 'paused', 'lease_expires_at', 'conversations',
})
AUTO_REPLY_SOURCE_REF_RE = re.compile(r'^[A-Za-z0-9._:-]{1,128}$')
AUTO_REPLY_TARGET_RE = re.compile(r'^(friend|group):[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$')
MAX_AUTO_REPLY_PERMIT_TTL_MS = 30_000
MANUAL_REPLY_VIEW_KEYS = frozenset({
    'schema_version',
    'account_sha256',
    'enabled',
    'manual_reply_enabled',
    'max_age_seconds',
    'paused',
    'lease_expires_at',
})
FROZEN_JOB_INVARIANT_FIELDS = (
    'schema_version', 'channel', 'kind', 'job_id', 'account_sha256',
    'contact_id', 'contact_type', 'recipient_id', 'recipient_alias',
    'group_id', 'group_name', 'group_alias', 'binding_sha256',
    'text', 'text_sha256', 'created_at', 'expires_at', 'confirmation_request_id',
    'origin_notice_id', 'origin_notice_key',
    'origin_notice_observed_at', 'origin_notice_created_at',
    'auto_reply_task_id', 'auto_reply_policy_version', 'auto_reply_source',
)
HEX64_RE = re.compile(r'^[0-9a-f]{64}$')
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
SHANGHAI = ZoneInfo('Asia/Shanghai')


def sha(value):
    if isinstance(value, (bytes, bytearray)):
        return hashlib.sha256(value).hexdigest()
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def save(path, value):
    """Flush one receipt and its directory before returning."""
    path = Path(path)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, delete=False) as stream:
        os.chmod(stream.name, 0o600)
        json.dump(value, stream, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
        temporary = stream.name
    try:
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def document(path):
    path = Path(path)
    if path.is_symlink() or path.stat().st_size > 128 * 1024:
        raise ValueError('unsafe_document')
    value = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(value, dict):
        raise ValueError('invalid_document')
    return value


def _infer_root(path, root=None):
    if root is not None:
        return Path(root)
    p = Path(path)
    if len(p.parents) >= 3 and p.parent.name == 'outbox' and p.parents[1].name == 'adhoc':
        return p.parents[2]
    return p.parent


def default_contacts_path(root, config_path=None):
    if config_path is not None:
        return Path(config_path).with_name('octopus-contacts.json')
    return Path(root) / 'config' / 'octopus-contacts.json'


def resolve_anchor_file(root, raw_path):
    if not isinstance(raw_path, str) or not raw_path.startswith('/'):
        raise ValueError('invalid_anchor_path')
    root_path = Path(root)
    if raw_path.startswith(CONTAINER_MOMENTS_PREFIX):
        candidate = root_path / raw_path[len(CONTAINER_MOMENTS_PREFIX):]
    else:
        candidate = Path(raw_path)
    root_resolved = root_path.resolve()
    try:
        target_resolved = candidate.resolve()
    except OSError as exc:
        raise ValueError('missing_anchor_file') from exc
    if not target_resolved.is_relative_to(root_resolved):
        raise ValueError('unsafe_anchor_path')
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError('missing_or_unsafe_anchor_file')
    return str(candidate), sha(candidate.read_bytes())


def _load_task2_gamma_symbol(module_name, symbol_name):
    try:
        module = importlib.import_module(module_name)
        return getattr(module, symbol_name)
    except (ImportError, AttributeError) as exc:
        raise ValueError('gamma_task2_adapter_unavailable') from exc


def compute_binding_sha256(root, config, contact):
    """Compute the exact canonical binding digest matching broker operations.mjs."""
    def h(value):
        return '-' if value is None else sha(str(value))

    account_anchor_path = config.get('account_anchor') or None
    account_anchor_sha = resolve_anchor_file(root, account_anchor_path)[1] if account_anchor_path else None
    own_chat_anchor_path = config.get('own_chat_anchor') or None
    own_chat_anchor_sha = resolve_anchor_file(root, own_chat_anchor_path)[1] if own_chat_anchor_path else None

    role_data = {}
    for role in ANCHOR_ROLES:
        raw = contact.get(role) or None
        if raw:
            role_data[role] = (raw, resolve_anchor_file(root, raw)[1])
        else:
            role_data[role] = (None, None)

    display_name = (contact.get('display_name') or contact.get('recipient_query') or '').strip()
    parts = [
        config['account_id'],
        contact['id'],
        contact.get('type', 'friend'),
        contact['alias'],
        display_name,
        contact['recipient_id'],
        contact['recipient_query'],
        account_anchor_path,
        account_anchor_sha,
        own_chat_anchor_path,
        own_chat_anchor_sha,
        role_data['recipient_anchor'][0],
        role_data['recipient_anchor'][1],
        role_data['recipient_sidebar_anchor'][0],
        role_data['recipient_sidebar_anchor'][1],
        role_data['recipient_card_anchor'][0],
        role_data['recipient_card_anchor'][1],
    ]
    return sha(':'.join(h(item) for item in parts))


def compute_group_binding_sha256(root, config, contact):
    """Compute the exact canonical group binding digest matching broker operations.mjs."""
    def h(value):
        return '-' if value is None else sha(str(value))

    account_anchor_path = config.get('account_anchor') or None
    account_anchor_sha = resolve_anchor_file(root, account_anchor_path)[1] if account_anchor_path else None
    own_chat_anchor_path = config.get('own_chat_anchor') or None
    own_chat_anchor_sha = resolve_anchor_file(root, own_chat_anchor_path)[1] if own_chat_anchor_path else None
    group_anchor_path = contact.get('group_anchor') or None
    group_anchor_sha = resolve_anchor_file(root, group_anchor_path)[1] if group_anchor_path else None
    group_name_anchor_path = contact.get('group_name_anchor') or None
    group_name_anchor_sha = resolve_anchor_file(root, group_name_anchor_path)[1] if group_name_anchor_path else None

    raw_members = contact.get('group_member_anchors') or []
    members = []
    for idx in range(2):
        if idx < len(raw_members) and raw_members[idx]:
            members.append((raw_members[idx], resolve_anchor_file(root, raw_members[idx])[1]))
        else:
            members.append((None, None))

    group_name = (contact.get('group_name') or '').strip()
    display_name = (contact.get('display_name') or group_name).strip()
    group_query = (contact.get('group_query') or group_name).strip()
    read_mode = (contact.get('group_name_read_mode') or 'auto').strip()
    sidebar_width = int(contact.get('group_sidebar_width', 280))

    parts = [
        'group_v1',
        config['account_id'],
        contact['id'],
        'group',
        contact['alias'],
        display_name,
        contact['group_id'],
        group_name,
        group_query,
        read_mode,
        str(sidebar_width),
        account_anchor_path,
        account_anchor_sha,
        own_chat_anchor_path,
        own_chat_anchor_sha,
        group_anchor_path,
        group_anchor_sha,
        group_name_anchor_path,
        group_name_anchor_sha,
        members[0][0],
        members[0][1],
        members[1][0],
        members[1][1],
    ]
    return sha(':'.join(h(item) for item in parts))


def compute_adhoc_moments_binding_sha256(root, config, visibility, text_sha256, viewport_policy=None):
    """Compute canonical ad hoc Moments request binding digest matching broker operations.mjs."""
    def h(value):
        return '-' if value is None else sha(str(value))

    account_anchor_path = config.get('account_anchor') or None
    account_anchor_sha = resolve_anchor_file(root, account_anchor_path)[1] if account_anchor_path else None
    if viewport_policy is None:
        vp_class = _load_task2_gamma_symbol('gamma_cn.moments.desktop', 'AdhocViewportPolicy')
        viewport_policy = vp_class.from_config(config)
    vp_key = ','.join(str(v) for v in (
        viewport_policy.max_chars,
        viewport_policy.max_logical_lines,
        viewport_policy.min_window_width,
        viewport_policy.min_window_height,
        viewport_policy.cols_per_visual_line,
        viewport_policy.max_visual_lines,
        viewport_policy.max_unbroken_alnum_cols,
        viewport_policy.max_unbroken_token_cols,
        viewport_policy.max_composer_height_px,
    ))
    parts = [
        'adhoc_moments_v1',
        config['account_id'],
        account_anchor_path,
        account_anchor_sha,
        visibility,
        text_sha256,
        vp_key,
    ]
    return sha(':'.join(h(item) for item in parts))


def _validate_contacts_list(contacts, root):
    if not isinstance(contacts, list) or not 1 <= len(contacts) <= 50:
        raise ValueError('invalid_contacts_registry')
    seen_ids = set()
    seen_aliases = set()
    seen_recipient_ids = set()
    seen_group_ids = set()
    seen_anchor_paths = {role: set() for role in ANCHOR_ROLES}
    seen_anchor_shas = {role: set() for role in ANCHOR_ROLES}
    seen_group_anchor_paths = set()
    seen_group_anchor_shas = set()
    legacy_contacts = []
    by_id = {}
    for entry in contacts:
        if not isinstance(entry, dict):
            raise ValueError('invalid_contact_record')
        cid = entry.get('id')
        alias = entry.get('alias')
        ctype = entry.get('type')
        if not isinstance(cid, str) or not CONTACT_ID_RE.fullmatch(cid):
            raise ValueError('invalid_contact_id')
        if ctype not in {'friend', 'group'}:
            raise ValueError('unsupported_contact_type')
        if not isinstance(alias, str) or not alias.strip() or alias.strip() != alias:
            raise ValueError('invalid_contact_alias')
        if entry.get('enabled') is not True and entry.get('enabled') is not False:
            raise ValueError('invalid_contact_enabled')
        if cid in seen_ids or alias in seen_aliases:
            raise ValueError('duplicate_contact_identity')
        seen_ids.add(cid)
        seen_aliases.add(alias)

        if ctype == 'friend':
            rid = entry.get('recipient_id')
            if isinstance(rid, str) and rid.strip():
                if rid in seen_recipient_ids:
                    raise ValueError('duplicate_recipient_identity')
                seen_recipient_ids.add(rid)
            for role in ANCHOR_ROLES:
                raw = entry.get(role)
                if isinstance(raw, str) and raw.strip():
                    if raw in seen_anchor_paths[role]:
                        raise ValueError('duplicate_anchor_path')
                    seen_anchor_paths[role].add(raw)
                    if root is not None and entry.get('enabled') is True and entry.get('direct_enabled') is True:
                        try:
                            _, digest_val = resolve_anchor_file(root, raw)
                        except ValueError:
                            # An unbound contact's missing anchor blocks only jobs targeting that contact.
                            continue
                        if digest_val in seen_anchor_shas[role]:
                            raise ValueError('duplicate_anchor_digest')
                        seen_anchor_shas[role].add(digest_val)
            if entry.get('legacy') is True:
                legacy_contacts.append(entry)
        else:
            if entry.get('legacy') is True:
                raise ValueError('group_cannot_be_legacy')
            if entry.get('group_enabled') is not True and entry.get('group_enabled') is not False:
                raise ValueError('invalid_group_enabled')
            gid = entry.get('group_id')
            if isinstance(gid, str) and gid.strip():
                if gid in seen_group_ids:
                    raise ValueError('duplicate_group_identity')
                seen_group_ids.add(gid)
            g_anchor = entry.get('group_anchor')
            if isinstance(g_anchor, str) and g_anchor.strip():
                if g_anchor in seen_group_anchor_paths:
                    raise ValueError('duplicate_group_anchor_path')
                seen_group_anchor_paths.add(g_anchor)
                if root is not None and entry.get('enabled') is True and entry.get('group_enabled') is True:
                    try:
                        _, digest_val = resolve_anchor_file(root, g_anchor)
                    except ValueError:
                        digest_val = None
                    if digest_val is not None:
                        if digest_val in seen_group_anchor_shas:
                            raise ValueError('duplicate_group_anchor_digest')
                        seen_group_anchor_shas.add(digest_val)
            raw_members = entry.get('group_member_anchors')
            if raw_members is not None:
                if not isinstance(raw_members, list) or len(set(raw_members)) != len(raw_members):
                    raise ValueError('duplicate_group_member_anchor')
        by_id[cid] = entry
    if len(legacy_contacts) > 1:
        raise ValueError('multiple_legacy_contacts')
    return by_id, legacy_contacts[0] if legacy_contacts else None


def _load_sidecar_document(contacts_path, config, root, *, trusted_uid=0):
    contacts_path = Path(contacts_path)
    root_resolved = Path(root).resolve()
    try:
        target_resolved = contacts_path.resolve()
    except OSError as exc:
        raise ValueError('contacts_registry_missing') from exc
    if not target_resolved.is_relative_to(root_resolved):
        raise ValueError('unsafe_contacts_registry_path')
    metadata = contacts_path.lstat()
    if contacts_path.is_symlink() or not contacts_path.is_file() or metadata.st_uid != trusted_uid or (metadata.st_mode & 0o022):
        raise ValueError('untrusted_contacts_registry')
    sidecar = document(contacts_path)
    if sidecar.get('schema_version') != 1 or not isinstance(sidecar.get('contacts'), list):
        raise ValueError('invalid_contacts_registry')
    if 'account_sha256' in sidecar:
        pinned = sidecar['account_sha256']
        if not isinstance(pinned, str) or not HEX64_RE.fullmatch(pinned):
            raise ValueError('invalid_contacts_registry')
        account_id = config.get('account_id')
        if not isinstance(account_id, str) or not account_id.strip() or sha(account_id) != pinned:
            raise ValueError('account_binding_changed')
    adhoc_meta = sidecar.get('adhoc_moments')
    if adhoc_meta is not None:
        if not isinstance(adhoc_meta, dict) or not isinstance(adhoc_meta.get('enabled'), bool):
            raise ValueError('invalid_adhoc_moments_sidecar')
        vis = adhoc_meta.get('default_visibility')
        if vis is not None and vis not in {'private', 'friends'}:
            raise ValueError('invalid_adhoc_moments_sidecar')
    return sidecar


def load_contacts_sidecar(contacts_path, config, root, *, trusted_uid=0):
    return _load_sidecar_document(contacts_path, config, root, trusted_uid=trusted_uid)['contacts']


def _resolve_sidecar_file(root, *, contacts_path=None, config_path=None):
    if contacts_path is not None:
        return Path(contacts_path)
    candidate = default_contacts_path(root, config_path=config_path)
    return candidate if candidate.exists() else None


def _apply_local_evidence_dirs(effective, root, channel):
    if root is not None and str(root) != '/home/wechat/.gamma-moments':
        if not effective.get('evidence_dir'):
            effective['evidence_dir'] = str(Path(root) / 'receipts' / 'evidence')
        if channel == 'adhoc_moments' and not effective.get('adhoc_evidence_dir'):
            effective['adhoc_evidence_dir'] = str(Path(root) / 'adhoc' / 'receipts' / 'evidence')


def resolve_job_contact(job, config, *, root=None, contacts_path=None, config_path=None, trusted_uid=0):
    """Return an isolated single-target effective config and matched contact record."""
    if config.get('enabled') is not True:
        raise ValueError('identity_or_policy_changed')
    account_id = config.get('account_id')
    if not isinstance(account_id, str) or not account_id.strip():
        raise ValueError('identity_or_policy_changed')

    sidecar_file = _resolve_sidecar_file(root, contacts_path=contacts_path, config_path=config_path) if root is not None else contacts_path
    sidecar_doc = _load_sidecar_document(sidecar_file, config, root, trusted_uid=trusted_uid) if sidecar_file is not None else None
    contacts_list = sidecar_doc['contacts'] if sidecar_doc is not None else None

    channel = job.get('channel') or 'direct'
    if channel == 'group':
        if job['schema_version'] != 2 or job.get('contact_type') != 'group':
            raise ValueError('invalid_job_fields')
        if contacts_list is None:
            raise ValueError('contacts_registry_missing')
        by_id, _ = _validate_contacts_list(contacts_list, root)
        contact = by_id.get(job['contact_id'])
        if not contact or contact.get('type') != 'group':
            raise ValueError('unknown_contact')
        if contact.get('enabled') is not True:
            raise ValueError('target_disabled')
        if job['kind'] != 'probe':
            if config.get('group_enabled') is False or contact.get('group_enabled') is not True:
                raise ValueError('target_disabled')
        group_id = contact.get('group_id')
        group_name = contact.get('group_name')
        if not isinstance(group_id, str) or not group_id.strip() or not isinstance(group_name, str) or not group_name.strip():
            raise ValueError('group_binding_missing')
        read_mode = contact.get('group_name_read_mode') or 'auto'
        if read_mode not in GROUP_NAME_READ_MODES:
            raise ValueError('invalid_group_name_read_mode')
        sidebar_width = contact.get('group_sidebar_width', 280)
        if type(sidebar_width) is not int or not 200 <= sidebar_width <= 600:
            raise ValueError('invalid_group_sidebar_width')

        effective = dict(config)
        effective['group_id'] = group_id
        effective['group_name'] = group_name
        effective['group_query'] = (contact.get('group_query') or group_name).strip()
        effective['group_name_read_mode'] = read_mode
        effective['group_sidebar_width'] = sidebar_width
        effective['adhoc_job_id'] = job['job_id']

        if root is None:
            raise ValueError('missing_group_root')
        own_chat_raw = config.get('own_chat_anchor')
        if not own_chat_raw:
            raise ValueError('missing_own_chat_anchor')
        effective['own_chat_anchor'] = resolve_anchor_file(root, own_chat_raw)[0]
        if config.get('account_anchor'):
            effective['account_anchor'] = resolve_anchor_file(root, config['account_anchor'])[0]

        g_anchor = contact.get('group_anchor')
        if not g_anchor:
            raise ValueError('missing_group_anchor')
        effective['group_anchor'] = resolve_anchor_file(root, g_anchor)[0]

        raw_members = contact.get('group_member_anchors')
        if not isinstance(raw_members, list) or not 1 <= len(raw_members) <= 2:
            raise ValueError('invalid_group_member_anchors')
        resolved_members = []
        seen_member_digests = set()
        for raw_member in raw_members:
            member_path, member_digest = resolve_anchor_file(root, raw_member)
            if member_digest in seen_member_digests:
                raise ValueError('duplicate_group_member_anchor')
            seen_member_digests.add(member_digest)
            resolved_members.append(member_path)
        effective['group_member_anchors'] = resolved_members

        raw_name_anchor = contact.get('group_name_anchor')
        if read_mode == 'anchor' or raw_name_anchor:
            if not raw_name_anchor:
                raise ValueError('missing_group_name_anchor')
            effective['group_name_anchor'] = resolve_anchor_file(root, raw_name_anchor)[0]

        _apply_local_evidence_dirs(effective, root, 'group')
        return effective, contact

    if config.get('direct_enabled') is not True:
        raise ValueError('identity_or_policy_changed')
    if job['schema_version'] == 1:
        if contacts_list is not None:
            _, legacy_contact = _validate_contacts_list(contacts_list, root)
            if not legacy_contact or not config.get('recipient_id'):
                raise ValueError('legacy_recipient_missing')
            if (legacy_contact.get('recipient_id') != config['recipient_id']
                    or job['recipient_id'] != legacy_contact['recipient_id']):
                raise ValueError('legacy_recipient_mismatch')
            contact = legacy_contact
        else:
            if not config.get('recipient_id') or job['recipient_id'] != config['recipient_id']:
                raise ValueError('identity_or_policy_changed')
            contact = {
                'id': 'default',
                'type': 'friend',
                'alias': job['recipient_alias'],
                'display_name': config.get('recipient_query', job['recipient_alias']),
                'enabled': config.get('enabled') is True,
                'direct_enabled': config.get('direct_enabled') is True,
                'legacy': True,
                'recipient_id': config['recipient_id'],
                'recipient_query': config.get('recipient_query', ''),
                'recipient_anchor': config.get('recipient_anchor', ''),
                'recipient_sidebar_anchor': config.get('recipient_sidebar_anchor', ''),
                'recipient_card_anchor': config.get('recipient_card_anchor', ''),
            }
    elif job['schema_version'] == 2:
        if contacts_list is None:
            raise ValueError('contacts_registry_missing')
        by_id, _ = _validate_contacts_list(contacts_list, root)
        contact = by_id.get(job['contact_id'])
        if not contact:
            raise ValueError('unknown_contact')
        if contact.get('legacy') is True and config.get('recipient_id') != contact.get('recipient_id'):
            raise ValueError('legacy_recipient_mismatch')
    else:
        raise ValueError('invalid_job_fields')

    if contact.get('type') != 'friend':
        raise ValueError('unsupported_contact_type')
    if contact.get('enabled') is not True or contact.get('direct_enabled') is not True:
        raise ValueError('target_disabled')
    if not isinstance(contact.get('recipient_id'), str) or not contact['recipient_id'].strip():
        raise ValueError('recipient_binding_missing')

    effective = dict(config)
    effective['recipient_id'] = contact['recipient_id']
    effective['recipient_query'] = contact.get('recipient_query') or contact.get('display_name') or ''
    for role in ANCHOR_ROLES:
        raw = contact.get(role)
        if raw:
            if root is not None:
                resolved_path, _ = resolve_anchor_file(root, raw)
                effective[role] = resolved_path
            else:
                effective[role] = raw
        elif contacts_list is not None:
            raise ValueError('missing_contact_anchors')
    if root is not None:
        for global_role in ('account_anchor', 'own_chat_anchor'):
            raw = config.get(global_role)
            if raw:
                effective[global_role] = resolve_anchor_file(root, raw)[0]
    _apply_local_evidence_dirs(effective, root, 'direct')
    return effective, contact


def resolve_adhoc_moments_job(job, config, *, root=None, contacts_path=None, config_path=None, trusted_uid=0, viewport_policy=None):
    """Validate independent ad hoc Moments enablement, anchors, and job schema without mutating text."""
    if config.get('enabled') is not True or config.get('adhoc_moments_enabled') is False:
        raise ValueError('adhoc_moments_disabled')
    account_id = config.get('account_id')
    if not isinstance(account_id, str) or not account_id.strip():
        raise ValueError('identity_or_policy_changed')

    sidecar_file = _resolve_sidecar_file(root, contacts_path=contacts_path, config_path=config_path) if root is not None else contacts_path
    sidecar_doc = _load_sidecar_document(sidecar_file, config, root, trusted_uid=trusted_uid) if sidecar_file is not None else None
    sidecar_adhoc = sidecar_doc.get('adhoc_moments') if sidecar_doc is not None else None
    if sidecar_adhoc is not None:
        if sidecar_adhoc.get('enabled') is not True:
            raise ValueError('adhoc_moments_disabled')
    elif config.get('adhoc_moments_enabled') is not True:
        raise ValueError('adhoc_moments_disabled')

    validate_adhoc_job = _load_task2_gamma_symbol('gamma_cn.moments.desktop', 'validate_adhoc_moment_job')
    policy_cls = _load_task2_gamma_symbol('gamma_cn.moments.desktop', 'AdhocViewportPolicy')
    resolved_policy = viewport_policy or policy_cls.from_config(config)
    validate_adhoc_job(job, policy=resolved_policy)

    effective = dict(config)
    effective['adhoc_job_id'] = job['job_id']
    if root is not None and config.get('account_anchor'):
        effective['account_anchor'] = resolve_anchor_file(root, config['account_anchor'])[0]
    _apply_local_evidence_dirs(effective, root, 'adhoc_moments')
    return effective, {'visibility': job['visibility'], 'viewport_policy': resolved_policy}


def format_notice_id(notice_key):
    return 'wn_' + sha(notice_key)[:12]


def default_manual_reply_path(root, *, config_path=None, contacts_path=None):
    if contacts_path is not None:
        return Path(contacts_path).parent / 'octopus-manual-reply.json'
    if config_path is not None:
        return Path(config_path).parent / 'octopus-manual-reply.json'
    if root is not None:
        return Path(root) / 'config' / 'octopus-manual-reply.json'
    return None



def _read_trusted_worker_json(path, root, trusted_uid, reason):
    path = Path(path)
    try:
        root_path = Path(root).absolute()
        root_resolved = root_path.resolve()
        absolute = path.absolute()
        base = root_path if absolute.is_relative_to(root_path) else root_resolved
        resolved = path.resolve()
        if resolved != root_resolved / absolute.relative_to(base):
            raise ValueError(reason)
        fd = os.open(resolved, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as stream:
            metadata = os.fstat(stream.fileno())
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != trusted_uid
                    or metadata.st_mode & 0o027 or metadata.st_size > MAX_WORKER_CONFIG_BYTES):
                raise ValueError(reason)
            payload = stream.read(MAX_WORKER_CONFIG_BYTES + 1)
            if len(payload) > MAX_WORKER_CONFIG_BYTES:
                raise ValueError(reason)
            result = json.loads(payload)
        if not isinstance(result, dict):
            raise ValueError(reason)
        return result
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError(reason) from exc

def _read_trusted_manual_reply_view(
    config,
    *,
    root=None,
    config_path=None,
    contacts_path=None,
    manual_reply_path=None,
    trusted_uid=0,
):
    target = Path(manual_reply_path) if manual_reply_path is not None else default_manual_reply_path(
        root, config_path=config_path, contacts_path=contacts_path,
    )
    if target is None or (not target.exists() and not target.is_symlink()):
        return None
    if root is not None:
        root_resolved = Path(root).resolve()
        try:
            target_resolved = target.resolve()
        except OSError as exc:
            raise ValueError('manual_reply_disabled') from exc
        if not target_resolved.is_relative_to(root_resolved):
            raise ValueError('unsafe_manual_reply_config_path')
    metadata = target.lstat()
    if target.is_symlink() or not target.is_file() or metadata.st_uid != trusted_uid or (metadata.st_mode & 0o022):
        raise ValueError('untrusted_manual_reply_config')
    view = document(target)
    if set(view) != MANUAL_REPLY_VIEW_KEYS or view.get('schema_version') != 1:
        raise ValueError('invalid_manual_reply_config')
    account_id = config.get('account_id', '')
    if (not isinstance(view['account_sha256'], str)
            or not HEX64_RE.fullmatch(view['account_sha256'])
            or not isinstance(account_id, str)
            or not account_id.strip()
            or view['account_sha256'] != sha(account_id)):
        raise ValueError('account_binding_changed')
    if (type(view['enabled']) is not bool
            or type(view['manual_reply_enabled']) is not bool
            or type(view['paused']) is not bool):
        raise ValueError('invalid_manual_reply_config')
    lease_expires = view['lease_expires_at']
    if lease_expires is not None and (type(lease_expires) is not int or lease_expires <= 0):
        raise ValueError('invalid_manual_reply_config')
    max_age_sec = view['max_age_seconds']
    if type(max_age_sec) is not int or not 60 <= max_age_sec <= 86400:
        raise ValueError('invalid_manual_reply_config')
    return view


def resolve_manual_reply_authorization(
    job,
    config,
    now_ms,
    *,
    root=None,
    config_path=None,
    contacts_path=None,
    manual_reply_path=None,
    trusted_uid=0,
):
    view = _read_trusted_manual_reply_view(
        config,
        root=root,
        config_path=config_path,
        contacts_path=contacts_path,
        manual_reply_path=manual_reply_path,
        trusted_uid=trusted_uid,
    )
    if view is None:
        raise ValueError('manual_reply_disabled')
    if view['account_sha256'] != job['account_sha256']:
        raise ValueError('account_binding_changed')
    if view['enabled'] is not True:
        raise ValueError('manual_reply_disabled')
    if view['paused'] is True:
        raise ValueError('manual_reply_paused')
    if view['manual_reply_enabled'] is not True:
        raise ValueError('manual_reply_disabled')
    lease_expires = view['lease_expires_at']
    if lease_expires is not None and now_ms >= lease_expires:
        raise ValueError('manual_reply_lease_expired')
    notice_deadline = min(job['origin_notice_observed_at'], job['origin_notice_created_at']) + view['max_age_seconds'] * 1000
    if now_ms >= notice_deadline:
        raise ValueError('manual_reply_notice_expired')
    return notice_deadline



def resolve_auto_reply_authorization(
    job,
    config,
    now_ms,
    *,
    root,
    config_path=None,
    contacts_path=None,
    manual_reply_path=None,
    trusted_uid=0,
):
    task_id = job['auto_reply_task_id']
    if (not isinstance(task_id, str) or not ADHOC_JOB_ID_RE.fullmatch(task_id)
            or task_id != job['job_id'] or job['confirmation_request_id'] != f'wechat-auto:{task_id}'
            or type(job['auto_reply_policy_version']) is not int or job['auto_reply_policy_version'] < 1):
        raise ValueError('invalid_auto_reply_job')
    source = job['auto_reply_source']
    if (not isinstance(source, dict) or set(source) != AUTO_REPLY_SOURCE_KEYS
            or not isinstance(source['source_ref'], str) or not AUTO_REPLY_SOURCE_REF_RE.fullmatch(source['source_ref'])
            or any(not isinstance(source[field], str) or not HEX64_RE.fullmatch(source[field])
                   for field in ('text_sha256', 'evidence_sha256'))
            or type(source['observed_at']) is not int
            or not MIN_OBSERVED_MS <= source['observed_at'] <= job['created_at']):
        raise ValueError('invalid_auto_reply_source')

    manual_view = _read_trusted_manual_reply_view(
        config,
        root=root,
        config_path=config_path,
        contacts_path=contacts_path,
        manual_reply_path=manual_reply_path,
        trusted_uid=trusted_uid,
    )
    if manual_view is None or manual_view['enabled'] is not True:
        raise ValueError('auto_reply_disabled')
    if manual_view['paused']:
        raise ValueError('auto_reply_paused')
    manual_lease = manual_view['lease_expires_at']
    if manual_lease is not None and now_ms >= manual_lease:
        raise ValueError('auto_reply_lease_expired')
    if now_ms >= source['observed_at'] + manual_view['max_age_seconds'] * 1000:
        raise ValueError('auto_reply_source_expired')
    permit_path = default_manual_reply_path(root, config_path=config_path, contacts_path=contacts_path)
    permit = _read_trusted_worker_json(
        permit_path.with_name('octopus-auto-reply.json'), root, trusted_uid, 'untrusted_auto_reply_permit',
    )
    expiry = permit.get('lease_expires_at')
    conversations = permit.get('conversations')
    if (set(permit) != AUTO_REPLY_VIEW_KEYS or type(permit['schema_version']) is not int or permit['schema_version'] != 1
            or permit['account_sha256'] != job['account_sha256']
            or type(permit['policy_version']) is not int or permit['policy_version'] != job['auto_reply_policy_version']
            or type(permit['enabled']) is not bool or type(permit['paused']) is not bool
            or type(expiry) is not int or not 0 < expiry - now_ms <= MAX_AUTO_REPLY_PERMIT_TTL_MS
            or not isinstance(conversations, dict)
            or any(not AUTO_REPLY_TARGET_RE.fullmatch(key) or mode != 'unattended' for key, mode in conversations.items())):
        raise ValueError('invalid_auto_reply_permit')
    if permit['enabled'] is not True or permit['paused'] is True:
        raise ValueError('auto_reply_disabled')
    target_key = f"{job['contact_type']}:{job['contact_id']}"
    if conversations.get(target_key) != 'unattended':
        raise ValueError('auto_reply_target_not_enabled')
    return True


def _assert_frozen_job_invariant(frozen_job, fresh_job):
    for field_name in FROZEN_JOB_INVARIANT_FIELDS:
        if fresh_job.get(field_name) != frozen_job.get(field_name):
            raise ValueError('binding_changed_before_submit')


def validate_job(
    path,
    config,
    now_ms,
    trusted_uid=0,
    *,
    root=None,
    contacts_path=None,
    config_path=None,
    viewport_policy=None,
    manual_reply_path=None,
):
    """Accept immutable root-owned jobs only; models cannot write this outbox."""
    path = Path(path)
    metadata = path.lstat()
    if path.is_symlink() or metadata.st_uid != trusted_uid or metadata.st_mode & 0o022:
        raise ValueError('untrusted_job_owner')
    job = document(path)
    version = job.get('schema_version')
    channel = job.get('channel')
    if version == 1 and channel is None:
        required = {'schema_version', 'kind', 'job_id', 'account_sha256', 'recipient_id', 'recipient_alias',
                    'text', 'text_sha256', 'created_at', 'expires_at', 'confirmation_request_id'}
        allowed_kinds = {'send', 'probe'}
        allow_notice_origin = False
    elif version == 2 and channel is None:
        required = {'schema_version', 'kind', 'job_id', 'account_sha256', 'contact_id', 'contact_type',
                    'recipient_id', 'recipient_alias', 'binding_sha256', 'text', 'text_sha256',
                    'created_at', 'expires_at', 'confirmation_request_id'}
        allowed_kinds = {'send', 'probe'}
        allow_notice_origin = True
    elif version == 2 and channel == 'group':
        required = {'schema_version', 'channel', 'kind', 'job_id', 'account_sha256', 'contact_id', 'contact_type',
                    'group_id', 'group_name', 'group_alias', 'binding_sha256', 'text', 'text_sha256',
                    'created_at', 'expires_at', 'confirmation_request_id'}
        allowed_kinds = {'send', 'probe'}
        allow_notice_origin = True
    elif version == 2 and channel == 'adhoc_moments':
        required = {'schema_version', 'channel', 'kind', 'job_id', 'account_sha256', 'visibility',
                    'binding_sha256', 'text', 'text_sha256', 'trade_date', 'created_at', 'expires_at',
                    'confirmation_request_id'}
        allowed_kinds = {'publish'}
        allow_notice_origin = False
    else:
        raise ValueError('invalid_job_fields')

    optional = NOTICE_REPLY_FIELDS | AUTO_REPLY_FIELDS if (allow_notice_origin and job.get('kind') == 'send') else set()
    job_keys = set(job)
    if not required.issubset(job_keys) or not job_keys.issubset(required | optional) or job['kind'] not in allowed_kinds:
        raise ValueError('invalid_job_fields')
    notice_keys_present = NOTICE_REPLY_FIELDS & job_keys
    if notice_keys_present and notice_keys_present != NOTICE_REPLY_FIELDS:
        raise ValueError('invalid_job_fields')
    has_notice_origin = bool(notice_keys_present)
    auto_keys_present = AUTO_REPLY_FIELDS & job_keys
    has_auto_origin = bool(auto_keys_present)
    if (has_auto_origin and (has_notice_origin or auto_keys_present != AUTO_REPLY_FIELDS)
            or not has_auto_origin and str(job.get('confirmation_request_id', '')).startswith('wechat-auto:')):
        raise ValueError('invalid_auto_reply_job')
    if has_notice_origin:
        origin_id = job['origin_notice_id']
        if not isinstance(origin_id, str) or not ORIGIN_NOTICE_ID_RE.fullmatch(origin_id):
            raise ValueError('invalid_origin_notice_id')
        origin_key = job['origin_notice_key']
        if not isinstance(origin_key, str) or not NOTICE_ORIGIN_KEY_RE.fullmatch(origin_key):
            raise ValueError('invalid_origin_notice_key')
        if format_notice_id(origin_key) != origin_id:
            raise ValueError('origin_notice_identity_mismatch')
        for ts_key in ('origin_notice_observed_at', 'origin_notice_created_at'):
            ts_val = job[ts_key]
            if type(ts_val) is not int or not 1 <= ts_val <= 4_102_444_800_000:
                raise ValueError('invalid_origin_notice_time')
    if not ADHOC_JOB_ID_RE.fullmatch(str(job['job_id'])) or path.name != job['job_id'] + '.json':
        raise ValueError('invalid_job_id')
    if (not isinstance(job['text'], str) or len(job['text'].encode('utf-8')) > 4096
            or sha(job['text']) != job['text_sha256'] or '\0' in job['text']):
        raise ValueError('invalid_text')
    if job['kind'] in {'send', 'publish'} and (
        not job['text'].strip() or not isinstance(job['confirmation_request_id'], str) or not job['confirmation_request_id']
    ):
        raise ValueError('missing_send_authorization')
    if type(job['created_at']) is not int or type(job['expires_at']) is not int:
        raise ValueError('invalid_time')
    if job['created_at'] > now_ms or job['expires_at'] <= now_ms or not 0 < job['expires_at'] - job['created_at'] <= 1800000:
        raise ValueError('expired')

    resolved_root = _infer_root(path, root)
    if job['account_sha256'] != sha(config.get('account_id', '')):
        raise ValueError('identity_or_policy_changed')
    if has_notice_origin:
        resolve_manual_reply_authorization(
            job,
            config,
            now_ms,
            root=resolved_root,
            config_path=config_path,
            contacts_path=contacts_path,
            manual_reply_path=manual_reply_path,
            trusted_uid=trusted_uid,
        )

    if has_auto_origin:
        resolve_auto_reply_authorization(
            job, config, now_ms, root=resolved_root, config_path=config_path,
            contacts_path=contacts_path, manual_reply_path=manual_reply_path, trusted_uid=trusted_uid,
        )

    if channel == 'adhoc_moments':
        if not isinstance(job['trade_date'], str) or not DATE_RE.fullmatch(job['trade_date']):
            raise ValueError('invalid_trade_date')
        if not HEX64_RE.fullmatch(str(job['binding_sha256'])):
            raise ValueError('invalid_binding_digest')
        _, meta = resolve_adhoc_moments_job(
            job, config, root=resolved_root, contacts_path=contacts_path, config_path=config_path,
            trusted_uid=trusted_uid, viewport_policy=viewport_policy,
        )
        expected_binding = compute_adhoc_moments_binding_sha256(
            resolved_root, config, job['visibility'], job['text_sha256'], meta['viewport_policy'],
        )
        if job['binding_sha256'] != expected_binding:
            raise ValueError('binding_digest_mismatch')
        return job

    if channel == 'group':
        if job['contact_type'] != 'group':
            raise ValueError('unsupported_contact_type')
        if not isinstance(job['group_alias'], str) or not job['group_alias'].strip():
            raise ValueError('invalid_group_alias')
        if not HEX64_RE.fullmatch(str(job['binding_sha256'])):
            raise ValueError('invalid_binding_digest')
        _, contact = resolve_job_contact(
            job, config, root=resolved_root, contacts_path=contacts_path, config_path=config_path, trusted_uid=trusted_uid,
        )
        if (job['group_id'] != contact['group_id']
                or job['group_name'] != contact['group_name']
                or job['group_alias'] != contact['alias']):
            raise ValueError('group_binding_mismatch')
        if job['binding_sha256'] != compute_group_binding_sha256(resolved_root, config, contact):
            raise ValueError('binding_digest_mismatch')
        return job

    if not isinstance(job['recipient_alias'], str) or not job['recipient_alias'].strip():
        raise ValueError('invalid_recipient_alias')
    _, contact = resolve_job_contact(
        job, config, root=resolved_root, contacts_path=contacts_path, config_path=config_path, trusted_uid=trusted_uid,
    )
    if version == 2:
        if job['contact_type'] != 'friend':
            raise ValueError('unsupported_contact_type')
        if not HEX64_RE.fullmatch(str(job['binding_sha256'])):
            raise ValueError('invalid_binding_digest')
        if job['recipient_id'] != contact['recipient_id'] or job['recipient_alias'] != contact['alias']:
            raise ValueError('recipient_binding_mismatch')
        if job['binding_sha256'] != compute_binding_sha256(resolved_root, config, contact):
            raise ValueError('binding_digest_mismatch')
    return job


def label_rows(rows, image, template, matcher):
    """Restore only the pinned non-identity label when exact pixel matching proves its presence."""
    if any(row['text'] == '朋友资料' for row in rows):
        return rows
    try:
        box = matcher(image, template)
    except Exception:
        return rows
    return rows + [{'text': '朋友资料', 'box': box}]


def compatible_desktop(desktop_class, friend_label_anchor):
    if not friend_label_anchor:
        return desktop_class

    class CompatibleDesktop(desktop_class):
        def capture(self, window):
            image, rows = super().capture(window)
            if window['box'][2] == 292 and not any(row['text'] == '朋友资料' for row in rows):
                from PIL import Image
                import numpy as np
                from gamma_cn.moments.desktop import find_template
                template = np.array(Image.open(friend_label_anchor).convert('RGB'))
                rows = label_rows(rows, image, template, find_template)
            if window['box'][2] == 292:
                rows = recipient_id_rows(rows, self.config['recipient_id'])
            return image, rows

    return CompatibleDesktop


def recipient_id_rows(rows, expected):
    """Target the separate ID value row for copying; clipboard equality remains authoritative."""
    labels = [row for row in rows if '微信号' in row['text']]
    if len(labels) != 1:
        return rows
    label = labels[0]
    x, y, width, height = label['box']
    values = [row for row in rows if row is not label and row['text'].strip() == expected
              and row['box'][0] >= x + width
              and row['box'][0] - x - width <= 40
              and abs(row['box'][1] + row['box'][3] / 2 - y - height / 2) <= 5]
    if len(values) != 1:
        return rows
    return [{**row, 'box': values[0]['box']} if row is label else row for row in rows]


def _resolve_desktop_class(channel, direct_cls, group_cls, adhoc_cls, friend_label_anchor):
    if channel == 'direct':
        return compatible_desktop(direct_cls, friend_label_anchor)
    if channel == 'group':
        if group_cls is False:
            raise ValueError('gamma_task2_adapter_unavailable')
        if group_cls is None:
            group_cls = _load_task2_gamma_symbol('gamma_cn.moments.group', 'GroupDesktop')
        return group_cls
    if channel == 'adhoc_moments':
        if adhoc_cls is False:
            raise ValueError('gamma_task2_adapter_unavailable')
        if adhoc_cls is None:
            adhoc_cls = _load_task2_gamma_symbol('gamma_cn.moments.desktop', 'AdhocMomentsDesktop')
        return adhoc_cls
    raise ValueError('invalid_channel')


def process_job(
    path,
    root,
    config,
    queue_class,
    run_one,
    desktop_class,
    clock=time.time,
    trusted_uid=0,
    friend_label_anchor=None,
    config_path=None,
    contacts_path=None,
    group_desktop_class=None,
    adhoc_desktop_class=None,
    adhoc_viewport_policy=None,
    manual_reply_path=None,
):
    """Run once under Gamma's existing account lock; uncertain jobs never auto-replay."""
    root = Path(root)
    receipts = root / 'adhoc' / 'receipts'
    receipts.mkdir(parents=True, exist_ok=True, mode=0o700)
    receipt = receipts / (Path(path).stem + '.json')
    previous = document(receipt) if receipt.exists() else None
    if previous:
        if previous.get('state') in {'preparing', 'prepared', 'publishing'}:
            previous.update(state='needs_review', reason='worker_stopped_during_ui_transaction')
            save(receipt, previous)
        return previous
    try:
        job = validate_job(
            path, config, int(clock() * 1000), trusted_uid,
            root=root, contacts_path=contacts_path, config_path=config_path,
            viewport_policy=adhoc_viewport_policy, manual_reply_path=manual_reply_path,
        )
        channel = job.get('channel') or 'direct'
        if channel == 'adhoc_moments':
            effective_config, frozen_meta = resolve_adhoc_moments_job(
                job, config, root=root, contacts_path=contacts_path, config_path=config_path,
                trusted_uid=trusted_uid, viewport_policy=adhoc_viewport_policy,
            )
            frozen_contact = None
        else:
            effective_config, frozen_contact = resolve_job_contact(
                job, config, root=root, contacts_path=contacts_path, config_path=config_path, trusted_uid=trusted_uid,
            )
            frozen_meta = None
        auto_verifier = None
        auto_target = None
        if 'auto_reply_task_id' in job:
            scanner_module = importlib.import_module('gamma_cn.moments.inbound_scanner')
            auto_verifier = getattr(scanner_module, 'verify_reply_tail', None)
            if (not callable(auto_verifier)
                    or job['contact_type'] not in getattr(scanner_module, 'REPLY_TAIL_TARGET_KINDS', ())):
                raise ValueError('auto_reply_tail_adapter_unavailable')
            auto_target = VerifiedScanTarget(
                kind=job['contact_type'],
                target_id=job['contact_id'],
                target_key=f"{job['contact_type']}:{job['contact_id']}",
                account_sha256=job['account_sha256'],
                binding_sha256=job['binding_sha256'],
                display_name=frozen_contact.get('display_name') or frozen_contact['alias'],
                effective_config=effective_config,
            )

        selected_desktop = _resolve_desktop_class(
            channel, desktop_class, group_desktop_class, adhoc_desktop_class, friend_label_anchor,
        )
    except Exception as error:
        result = {'state': 'refused', 'reason': str(error)[:100]}
        save(receipt, result)
        return result

    def current_config():
        return document(config_path) if config_path is not None else config

    shared = queue_class(root / 'outbox', root / 'receipts')

    if job['kind'] == 'probe':
        try:
            with shared.lock(job['account_sha256']):
                fresh_cfg = current_config()
                fresh = validate_job(
                    path, fresh_cfg, int(clock() * 1000), trusted_uid,
                    root=root, contacts_path=contacts_path, config_path=config_path,
                )
                probe_cfg, probe_contact = resolve_job_contact(
                    fresh, fresh_cfg, root=root, contacts_path=contacts_path, config_path=config_path, trusted_uid=trusted_uid,
                )
                if channel == 'group':
                    desktop = selected_desktop(probe_cfg)
                    if desktop.identity() != job['account_sha256']:
                        raise ValueError('account_not_verified')
                    probe_data = desktop.probe(probe_contact['group_id'])
                    result = {
                        'state': 'identity_verified',
                        'channel': 'group',
                        'contact_id': probe_contact['id'],
                        'group_id': probe_data['group_id'],
                        'group_alias': job['group_alias'],
                        'group_name_sha256': probe_data['group_name_sha256'],
                        'group_detail_labels': list(probe_data['group_detail_labels']),
                        'member_anchor_sha256s': list(probe_data['member_anchor_sha256s']),
                        'group_name_evidence': probe_data['group_name_evidence'],
                        'account_sha256': probe_data['account_sha256'],
                        'checked_at': clock(),
                    }
                else:
                    desktop = selected_desktop(probe_cfg)
                    if desktop.identity() != job['account_sha256']:
                        raise ValueError('account_not_verified')
                    desktop._open_recipient(probe_contact['recipient_id'])
                    result = {'state': 'identity_verified', 'recipient_alias': job['recipient_alias'], 'checked_at': clock()}
                    if 'contact_id' in job:
                        result['contact_id'] = job['contact_id']
        except BlockingIOError:
            return {'state': 'busy'}
        except Exception as error:
            reason = str(error)
            result = {'state': 'auth_required', 'reason': reason if re.fullmatch('[a-z_]{1,100}', reason) else type(error).__name__}
        save(receipt, result)
        return result

    class AdhocQueue(queue_class):
        def load(self, key):
            if key != job['job_id']:
                raise ValueError('job_identity_mismatch')
            return document(receipt) if receipt.exists() else {'state': 'queued', 'attempts': 0}

        def save(self, key, record):
            if key != job['job_id']:
                raise ValueError('job_identity_mismatch')
            save(receipt, record)

        def read_job(self, key):
            fresh_cfg = current_config()
            fresh = validate_job(
                path, fresh_cfg, int(clock() * 1000), trusted_uid,
                root=root, contacts_path=contacts_path, config_path=config_path,
                viewport_policy=adhoc_viewport_policy, manual_reply_path=manual_reply_path,
            )
            _assert_frozen_job_invariant(job, fresh)
            trade_date = fresh.get('trade_date') or datetime.fromtimestamp(fresh['created_at'] / 1000, SHANGHAI).date().isoformat()
            if channel == 'adhoc_moments':
                _, fresh_meta = resolve_adhoc_moments_job(
                    fresh, fresh_cfg, root=root, contacts_path=contacts_path, config_path=config_path,
                    trusted_uid=trusted_uid, viewport_policy=adhoc_viewport_policy,
                )
                if fresh_meta['visibility'] != frozen_meta['visibility']:
                    raise ValueError('adhoc_moments_binding_changed')
                return {**fresh, 'channel': 'adhoc_moments', 'trade_date': trade_date}
            _, fresh_contact = resolve_job_contact(
                fresh, fresh_cfg, root=root, contacts_path=contacts_path, config_path=config_path, trusted_uid=trusted_uid,
            )
            if channel == 'group':
                if (fresh_contact['id'] != frozen_contact['id']
                        or fresh_contact['group_id'] != frozen_contact['group_id']
                        or fresh_contact['group_name'] != frozen_contact['group_name']
                        or fresh_contact['alias'] != job['group_alias']):
                    raise ValueError('group_binding_changed')
                return {**fresh, 'channel': 'group', 'trade_date': trade_date}
            if (fresh_contact['id'] != frozen_contact['id']
                    or fresh_contact['recipient_id'] != frozen_contact['recipient_id']
                    or (job['schema_version'] == 2 and fresh_contact['alias'] != job['recipient_alias'])):
                raise ValueError('recipient_binding_changed')
            return {**fresh, 'channel': 'direct', 'visibility': 'private', 'trade_date': trade_date}

        def lock(self, key, *, producer=False):
            return shared.lock(key, producer=producer)

    class TimedDesktop(selected_desktop):
        def _recheck_binding_before_ui(self, expiry_reason):
            now_ms = int(clock() * 1000)
            if now_ms >= job['expires_at']:
                raise ValueError(expiry_reason)
            fresh_cfg = current_config()
            try:
                fresh = validate_job(
                    path, fresh_cfg, now_ms, trusted_uid,
                    root=root, contacts_path=contacts_path, config_path=config_path,
                    viewport_policy=adhoc_viewport_policy, manual_reply_path=manual_reply_path,
                )
            except ValueError as exc:
                if str(exc) in {'expired', 'manual_reply_notice_expired'}:
                    raise ValueError(expiry_reason) from exc
                raise
            _assert_frozen_job_invariant(job, fresh)
            if channel == 'adhoc_moments':
                _, fresh_meta = resolve_adhoc_moments_job(
                    fresh, fresh_cfg, root=root, contacts_path=contacts_path, config_path=config_path,
                    trusted_uid=trusted_uid, viewport_policy=adhoc_viewport_policy,
                )
                if fresh_meta['visibility'] != frozen_meta['visibility']:
                    raise ValueError('binding_changed_before_submit')
                return
            _, fresh_contact = resolve_job_contact(
                fresh, fresh_cfg, root=root, contacts_path=contacts_path, config_path=config_path, trusted_uid=trusted_uid,
            )
            if channel == 'group':
                if (fresh_contact['id'] != frozen_contact['id']
                        or fresh_contact['group_id'] != frozen_contact['group_id']
                        or fresh_contact['group_name'] != frozen_contact['group_name']
                        or fresh_contact['alias'] != job['group_alias']
                        or self.config.get('group_id') != fresh_contact['group_id']
                        or self.config.get('group_name') != fresh_contact['group_name']):
                    raise ValueError('binding_changed_before_submit')
                return
            if (fresh_contact['id'] != frozen_contact['id']
                    or fresh_contact['recipient_id'] != frozen_contact['recipient_id']
                    or (job['schema_version'] == 2 and fresh_contact['alias'] != job['recipient_alias'])
                    or self.config.get('recipient_id') != fresh_contact['recipient_id']):
                raise ValueError('binding_changed_before_submit')

        def preflight(self):
            self._recheck_binding_before_ui('expired_before_preflight')
            ok = super().preflight()
            if ok is True and auto_verifier is not None:
                if auto_verifier(
                    auto_target, root=root, source=job['auto_reply_source'],
                    desktop=self, deadline_monotonic=time.monotonic() + 10,
                ) is not True:
                    raise ValueError('auto_reply_tail_not_verified')
            self._recheck_binding_before_ui('expired_before_submit')
            return ok

        def submit(self):
            self._recheck_binding_before_ui('expired_before_submit')
            return super().submit()

    queue = AdhocQueue(root / 'adhoc' / 'outbox', receipts)
    run_kwargs = {}
    if clock is not time.time and 'clock' in inspect.signature(run_one).parameters:
        run_kwargs['clock'] = lambda: datetime.fromtimestamp(clock(), SHANGHAI)
    try:
        return run_one(queue, job['job_id'], TimedDesktop(effective_config), **run_kwargs)
    except Exception as error:
        if not receipt.exists():
            result = {'state': 'refused', 'reason': str(error)[:100]}
            save(receipt, result)
            return result
        raise


def _task2_enabled_in_runtime(root, config, *, contacts_path=None, config_path=None, trusted_uid=0):
    if config.get('adhoc_moments_enabled') is True:
        return True
    sidecar_file = _resolve_sidecar_file(root, contacts_path=contacts_path, config_path=config_path)
    if sidecar_file is None:
        return False
    sidecar = _load_sidecar_document(sidecar_file, config, root, trusted_uid=trusted_uid)
    adhoc_meta = sidecar.get('adhoc_moments') or {}
    if adhoc_meta.get('enabled') is True and config.get('adhoc_moments_enabled') is not False:
        return True
    return any(
        c.get('type') == 'group' and c.get('enabled') is True and c.get('group_enabled') is True
        for c in sidecar.get('contacts', [])
    )


def _probe_optional_task2_adapters():
    try:
        from gamma_cn.moments.group import GroupDesktop
        from gamma_cn.moments.desktop import AdhocMomentsDesktop
        from gamma_cn.moments.delivery import AdhocMomentsQueue, CHANNEL_TARGETS
        if 'group' not in CHANNEL_TARGETS or 'adhoc_moments' not in CHANNEL_TARGETS:
            return False, False, False
        _ = AdhocMomentsQueue
        return GroupDesktop, AdhocMomentsDesktop, True
    except (ImportError, AttributeError):
        return False, False, False


def _safe_error_code(exc):
    reason = str(exc)
    return reason if re.fullmatch(r'[a-z0-9_]{1,100}', reason) else type(exc).__name__


def _checked_bounded_number(value, bounds, *, integer_only=False):
    if isinstance(value, bool):
        raise ValueError('invalid_inbound_scan_bounds')
    if integer_only:
        if not isinstance(value, int):
            raise ValueError('invalid_inbound_scan_bounds')
        num = value
    else:
        if not isinstance(value, (int, float)) or math.isnan(value) or math.isinf(value):
            raise ValueError('invalid_inbound_scan_bounds')
        num = float(value)
    low, high = bounds
    if num < low or num > high:
        raise ValueError('invalid_inbound_scan_bounds')
    return num


@dataclass(frozen=True)
class RegisteredOwnPostTarget:
    """Explicitly preregistered own Moments post target; never inferred from feed OCR."""

    own_post_id: str
    enabled: bool = True
    account_sha256: Optional[str] = None
    post_label: Optional[str] = None

    def __post_init__(self):
        if not isinstance(self.own_post_id, str) or self.own_post_id == '*' or not MOMENTS_REF_RE.fullmatch(self.own_post_id):
            raise ValueError('invalid_registered_own_post')
        if type(self.enabled) is not bool:
            raise ValueError('invalid_registered_own_post')
        if self.account_sha256 is not None:
            if not isinstance(self.account_sha256, str) or not HEX64_RE.fullmatch(self.account_sha256):
                raise ValueError('invalid_registered_own_post')
        if self.post_label is not None:
            if (not isinstance(self.post_label, str)
                    or not self.post_label.strip()
                    or '\0' in self.post_label
                    or len(self.post_label.encode('utf-8')) > 128):
                raise ValueError('invalid_registered_own_post')

    @classmethod
    def from_entry(cls, raw):
        if isinstance(raw, cls):
            return raw
        if isinstance(raw, str):
            return cls(own_post_id=raw)
        if not isinstance(raw, dict):
            raise ValueError('invalid_registered_own_post')
        allowed = {'own_post_id', 'enabled', 'account_sha256', 'post_label'}
        if not set(raw).issubset(allowed) or 'own_post_id' not in raw:
            raise ValueError('invalid_registered_own_post')
        return cls(
            own_post_id=raw['own_post_id'],
            enabled=raw.get('enabled', True),
            account_sha256=raw.get('account_sha256'),
            post_label=raw.get('post_label'),
        )



GROUP_SCAN_ANCHOR_FIELDS = (
    'at_me_badge_anchor', 'at_me_session_group_anchor', 'visual_mention_anchor',
    'session_park_anchor', 'session_park_heading_anchor',
)


def _validate_group_scan_options(group_id, group):
    anchor_fields = set(GROUP_SCAN_ANCHOR_FIELDS)
    option_fields = {'registered_authors', 'sender_verify_mode', 'allow_registered_sender_mentions'}
    if (not isinstance(group_id, str) or not CONTACT_ID_RE.fullmatch(group_id)
            or not isinstance(group, dict) or set(group) - (anchor_fields | option_fields)):
        raise ValueError('invalid_group_inbound_config')
    if type(group.get('allow_registered_sender_mentions', False)) is not bool:
        raise ValueError('invalid_registered_sender_mentions_flag')
    if group.get('sender_verify_mode', 'auto') not in ('auto', 'pinned_only', 'card_copy'):
        raise ValueError('invalid_group_sender_verify_mode')
    park_fields = {'session_park_anchor', 'session_park_heading_anchor'}
    if park_fields & set(group) and not (park_fields | {'at_me_badge_anchor', 'at_me_session_group_anchor'}).issubset(group):
        raise ValueError('incomplete_group_scan_parking_config')
    if group.get('allow_registered_sender_mentions') and not park_fields.issubset(group):
        raise ValueError('registered_sender_mentions_require_parked_scan')
    for name in anchor_fields & set(group):
        path = group[name]
        if not isinstance(path, str) or not path.startswith('/'):
            raise ValueError('invalid_group_inbound_anchor')
    authors = group.get('registered_authors', [])
    if not isinstance(authors, list) or len(authors) > 128:
        raise ValueError('invalid_group_inbound_config')
    allowed_text = {'author_id', 'display_name', 'wechat_id', 'avatar_anchor', 'card_anchor'}
    allowed_flags = {'is_self', 'is_automated'}
    for author in authors:
        if (not isinstance(author, dict) or not author.get('author_id')
                or set(author) - (allowed_text | allowed_flags)
                or any(not isinstance(author[key], str) or not author[key].strip()
                       or '\0' in author[key] or len(author[key].encode('utf-8')) > 512
                       for key in allowed_text & set(author))
                or any(type(author[key]) is not bool for key in allowed_flags & set(author))
                or any(not author[key].startswith('/') for key in {'avatar_anchor', 'card_anchor'} & set(author))):
            raise ValueError('invalid_group_inbound_author')


def _verified_group_scan_options(root, group, *, trusted_uid):
    def trusted_anchor(path):
        resolved, _ = resolve_anchor_file(root, path)
        metadata = Path(resolved).stat()
        if metadata.st_uid != trusted_uid or metadata.st_mode & 0o022:
            raise ValueError('untrusted_group_inbound_anchor')
        return resolved

    result = {}
    for field in GROUP_SCAN_ANCHOR_FIELDS:
        if field in group:
            result[field] = trusted_anchor(group[field])
    if 'sender_verify_mode' in group:
        result['sender_verify_mode'] = group['sender_verify_mode']
    if 'allow_registered_sender_mentions' in group:
        result['allow_registered_sender_mentions'] = group['allow_registered_sender_mentions']
    if 'registered_authors' in group:
        result['registered_authors'] = []
        for author in group['registered_authors']:
            author = dict(author)
            for field in ('avatar_anchor', 'card_anchor'):
                if field in author:
                    author[field] = trusted_anchor(author[field])
            result['registered_authors'].append(author)
    return result


@dataclass(frozen=True)
class InboundScanConfig:
    """Default-off bounded configuration for staged single-target inbound scan slices."""

    schema_version: int = INBOUND_SCAN_SCHEMA_VERSION
    enabled: bool = False
    slice_budget_seconds: float = 5.0
    idle_scan_interval_seconds: float = 10.0
    fallback_interval_seconds: float = 60.0
    fairness_max_deferral_seconds: float = 30.0
    fairness_max_outbound_ticks: Optional[int] = None
    use_unread_hint: bool = True
    contact_ids: Optional[Tuple[str, ...]] = None
    registered_own_posts: Tuple[Union[RegisteredOwnPostTarget, str], ...] = ()
    self_mention_names: Tuple[str, ...] = ()
    group_inbound: Optional[dict] = None

    def __post_init__(self):
        if type(self.schema_version) is not int or self.schema_version != INBOUND_SCAN_SCHEMA_VERSION:
            raise ValueError('invalid_inbound_scan_config')
        if type(self.enabled) is not bool or type(self.use_unread_hint) is not bool:
            raise ValueError('invalid_inbound_scan_config')
        slice_budget = _checked_bounded_number(
            self.slice_budget_seconds, INBOUND_SCAN_BOUNDS['slice_budget_seconds'],
        )
        idle_interval = _checked_bounded_number(
            self.idle_scan_interval_seconds, INBOUND_SCAN_BOUNDS['idle_scan_interval_seconds'],
        )
        fallback_interval = _checked_bounded_number(
            self.fallback_interval_seconds, INBOUND_SCAN_BOUNDS['fallback_interval_seconds'],
        )
        if fallback_interval < idle_interval:
            raise ValueError('invalid_inbound_scan_bounds')
        fairness_deferral = _checked_bounded_number(
            self.fairness_max_deferral_seconds, INBOUND_SCAN_BOUNDS['fairness_max_deferral_seconds'],
        )
        fairness_ticks = None
        if self.fairness_max_outbound_ticks is not None:
            fairness_ticks = _checked_bounded_number(
                self.fairness_max_outbound_ticks,
                INBOUND_SCAN_BOUNDS['fairness_max_outbound_ticks'],
                integer_only=True,
            )
        if self.contact_ids is not None:
            if (not isinstance(self.contact_ids, (tuple, list)) or len(self.contact_ids) > 50
                    or any(not isinstance(cid, str) or not CONTACT_ID_RE.fullmatch(cid) for cid in self.contact_ids)
                    or len(set(self.contact_ids)) != len(self.contact_ids)):
                raise ValueError('invalid_inbound_contact_ids')
            object.__setattr__(self, 'contact_ids', tuple(self.contact_ids))
        if not isinstance(self.registered_own_posts, (tuple, list)):
            raise ValueError('invalid_registered_own_post')
        if len(self.registered_own_posts) > MAX_REGISTERED_OWN_POSTS:
            raise ValueError('invalid_registered_own_post')
        normalized_posts = []
        seen_posts = set()
        for entry in self.registered_own_posts:
            item = RegisteredOwnPostTarget.from_entry(entry)
            if item.own_post_id in seen_posts:
                raise ValueError('duplicate_registered_own_post')
            seen_posts.add(item.own_post_id)
            normalized_posts.append(item)

        raw_names = self.self_mention_names
        if (not isinstance(raw_names, (tuple, list)) or len(raw_names) > 16
                or any(not isinstance(name, str) or not name.strip()
                       or len(name.encode('utf-8')) > 128
                       or any(ord(char) < 32 or char == '\u2005' for char in name)
                       for name in raw_names)):
            raise ValueError('invalid_self_mention_names')
        groups = self.group_inbound
        if groups is None:
            groups = {}
        if not isinstance(groups, dict) or len(groups) > 64:
            raise ValueError('invalid_group_inbound_config')
        for group_id, group in groups.items():
            _validate_group_scan_options(group_id, group)
        object.__setattr__(self, 'self_mention_names', tuple(name.strip() for name in raw_names))
        object.__setattr__(self, 'group_inbound', groups)

        object.__setattr__(self, 'slice_budget_seconds', slice_budget)
        object.__setattr__(self, 'idle_scan_interval_seconds', idle_interval)
        object.__setattr__(self, 'fallback_interval_seconds', fallback_interval)
        object.__setattr__(self, 'fairness_max_deferral_seconds', fairness_deferral)
        object.__setattr__(self, 'fairness_max_outbound_ticks', fairness_ticks)
        object.__setattr__(self, 'registered_own_posts', tuple(normalized_posts))

    @classmethod
    def from_mapping(cls, raw):
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError('invalid_inbound_scan_config')
        allowed = {
            'schema_version',
            'enabled',
            'slice_budget_seconds',
            'idle_scan_interval_seconds',
            'fallback_interval_seconds',
            'fairness_max_deferral_seconds',
            'fairness_max_outbound_ticks',
            'use_unread_hint',
            'contact_ids',
            'registered_own_posts',
            'self_mention_names',
            'group_inbound',
        }
        unknown = set(raw) - allowed
        if unknown:
            raise ValueError('unknown_inbound_scan_config_field')
        return cls(**raw)


@dataclass(frozen=True)
class VerifiedScanTarget:
    """One verified root-registered target eligible for a single-target scan slice."""

    kind: str
    target_id: str
    target_key: str
    account_sha256: str
    binding_sha256: Optional[str]
    display_name: str
    effective_config: dict


@dataclass(frozen=True)
class UnreadHintResult:
    """Verified optional pre-screen unread hint bounded to already-registered targets."""

    schema_version: int = INBOUND_SCAN_SCHEMA_VERSION
    verified: bool = False
    account_sha256: str = ''
    has_unread: bool = False
    target_keys: Tuple[str, ...] = ()
    observed_at_ms: Optional[int] = None
    reason: Optional[str] = None


@dataclass(frozen=True)
class ScanSliceResult:
    """Structured result from one bounded single-target scan slice."""

    target_key: str
    observations: tuple = ()
    emitted_event_ids: Tuple[str, ...] = ()
    ui_restored: bool = True
    draft_present: bool = False
    needs_review: bool = False
    reason: Optional[str] = None


def validate_inbound_scan_adapter(adapter):
    """Require explicit calibrated capability advertisement before accepting an inbound scanner."""
    if adapter is None:
        raise ValueError('gamma_inbound_scan_adapter_unavailable')
    cap_attr = getattr(adapter, 'inbound_scan_capability', None)
    if callable(cap_attr):
        cap = cap_attr()
    else:
        cap = getattr(adapter, 'INBOUND_SCAN_CAPABILITY', None)
    if not isinstance(cap, dict):
        raise ValueError('gamma_inbound_scan_adapter_unavailable')
    if (cap.get('schema_version') != INBOUND_SCAN_SCHEMA_VERSION
            or cap.get('supported') is not True
            or cap.get('calibrated') is not True):
        raise ValueError('gamma_inbound_scan_adapter_unavailable')
    kinds = cap.get('supported_target_kinds')
    if (not isinstance(kinds, (tuple, list, set, frozenset))
            or not kinds
            or any(k not in INBOUND_SCAN_TARGET_KINDS for k in kinds)):
        raise ValueError('gamma_inbound_scan_adapter_unavailable')
    supports_unread_hint = cap.get('supports_unread_hint', False)
    if type(supports_unread_hint) is not bool:
        raise ValueError('gamma_inbound_scan_adapter_unavailable')
    for required_method in ('verify_account_identity', 'scan_target', 'restore_ui'):
        if not callable(getattr(adapter, required_method, None)):
            raise ValueError('gamma_inbound_scan_adapter_unavailable')
    if supports_unread_hint and not callable(getattr(adapter, 'read_unread_hint', None)):
        raise ValueError('gamma_inbound_scan_adapter_unavailable')
    return {
        'schema_version': INBOUND_SCAN_SCHEMA_VERSION,
        'supported': True,
        'calibrated': True,
        'supported_target_kinds': frozenset(kinds),
        'supports_unread_hint': supports_unread_hint,
    }


def _probe_optional_inbound_scan_adapter():
    """Return an explicitly calibrated Gamma inbound scanner if present, else unavailable."""
    try:
        module = importlib.import_module('gamma_cn.moments.inbound_scanner')
        factory = getattr(module, 'create_inbound_scanner', None) or getattr(module, 'InboundScannerAdapter', None)
        if not callable(factory):
            return None, False
        adapter = factory()
        validate_inbound_scan_adapter(adapter)
        return adapter, True
    except (ImportError, AttributeError, ValueError, TypeError):
        return None, False


def _is_anchor_symlink(root_path, raw_path):
    if not isinstance(raw_path, str) or not raw_path.startswith('/'):
        return False
    candidate = (root_path / raw_path[len(CONTAINER_MOMENTS_PREFIX):]) if raw_path.startswith(CONTAINER_MOMENTS_PREFIX) else Path(raw_path)
    return candidate.is_symlink()


def resolve_inbound_scan_targets(
    root,
    config,
    *,
    trusted_uid=0,
    config_path=None,
    contacts_path=None,
    manual_reply_path=None,
    scan_config=None,
):
    """Resolve root-owned config, contacts, and registered own posts into verified scan targets."""
    cfg_model = scan_config if isinstance(scan_config, InboundScanConfig) else InboundScanConfig.from_mapping(scan_config)
    if cfg_model.enabled is not True:
        return {'state': 'disabled', 'reason': 'inbound_scan_disabled', 'account_sha256': None, 'targets': ()}
    if config_path is not None:
        cfg_file = Path(config_path)
        if cfg_file.is_symlink() or not cfg_file.is_file():
            raise ValueError('unsafe_worker_config')
    if not isinstance(config, dict):
        raise ValueError('invalid_worker_config')
    if config.get('enabled') is not True:
        return {'state': 'disabled', 'reason': 'worker_disabled', 'account_sha256': None, 'targets': ()}
    account_id = config.get('account_id')
    if not isinstance(account_id, str) or not account_id.strip():
        raise ValueError('identity_or_policy_changed')
    account_sha256 = sha(account_id)

    view = _read_trusted_manual_reply_view(
        config,
        root=root,
        config_path=config_path,
        contacts_path=contacts_path,
        manual_reply_path=manual_reply_path,
        trusted_uid=trusted_uid,
    )
    if view is None or view['enabled'] is not True:
        return {'state': 'disabled', 'reason': 'notifications_disabled', 'account_sha256': account_sha256, 'targets': ()}

    root_path = Path(root)
    resolved_global_anchors = {}
    for global_role in ('account_anchor', 'own_chat_anchor'):
        raw_global = config.get(global_role)
        if raw_global:
            try:
                resolved_global_anchors[global_role] = resolve_anchor_file(root_path, raw_global)[0]
            except ValueError as exc:
                if str(exc) in {'invalid_anchor_path', 'unsafe_anchor_path'} or _is_anchor_symlink(root_path, raw_global):
                    raise
                resolved_global_anchors[global_role] = None
        else:
            resolved_global_anchors[global_role] = None

    if contacts_path is not None:
        sidecar_candidate = Path(contacts_path)
        if not sidecar_candidate.exists() and not sidecar_candidate.is_symlink():
            raise ValueError('contacts_registry_missing')
        sidecar_file = sidecar_candidate
    else:
        default_sidecar = default_contacts_path(root_path, config_path=config_path)
        sidecar_file = default_sidecar if (default_sidecar.exists() or default_sidecar.is_symlink()) else None

    targets = []
    if sidecar_file is not None:
        sidecar_doc = _load_sidecar_document(sidecar_file, config, root_path, trusted_uid=trusted_uid)
        contacts_list = sidecar_doc['contacts']
        _validate_contacts_list(contacts_list, root_path)
        selected_ids = None if cfg_model.contact_ids is None else set(cfg_model.contact_ids)
        if selected_ids is not None and selected_ids - {contact['id'] for contact in contacts_list}:
            raise ValueError('unregistered_inbound_contact')
        for contact in contacts_list:
            if selected_ids is not None and contact['id'] not in selected_ids:
                continue
            if contact.get('enabled') is not True:
                continue
            ctype = contact.get('type')
            if ctype == 'friend':
                if contact.get('legacy') is True and config.get('recipient_id') != contact.get('recipient_id'):
                    continue
                rid = contact.get('recipient_id')
                rquery = (contact.get('recipient_query') or contact.get('display_name') or '').strip()
                if not isinstance(rid, str) or not rid.strip() or not rquery:
                    continue
                if config.get('account_anchor') and resolved_global_anchors['account_anchor'] is None:
                    continue
                if config.get('own_chat_anchor') and resolved_global_anchors['own_chat_anchor'] is None:
                    continue
                resolved_roles = {}
                missing_friend_anchor = False
                for role in ANCHOR_ROLES:
                    raw_role = contact.get(role)
                    if not raw_role:
                        missing_friend_anchor = True
                        break
                    try:
                        resolved_roles[role] = resolve_anchor_file(root_path, raw_role)[0]
                    except ValueError as exc:
                        if str(exc) in {'invalid_anchor_path', 'unsafe_anchor_path'} or _is_anchor_symlink(root_path, raw_role):
                            raise
                        missing_friend_anchor = True
                        break
                if missing_friend_anchor:
                    continue
                binding_sha256 = compute_binding_sha256(root_path, config, contact)
                effective = dict(config)
                effective['recipient_id'] = rid
                effective['recipient_query'] = rquery
                effective.update(resolved_roles)
                for global_role, resolved_path in resolved_global_anchors.items():
                    if resolved_path is not None:
                        effective[global_role] = resolved_path
                _apply_local_evidence_dirs(effective, root_path, 'direct')
                display_name = (contact.get('display_name') or contact.get('alias') or contact['id']).strip()
                targets.append(VerifiedScanTarget(
                    kind='friend',
                    target_id=contact['id'],
                    target_key=f"friend:{contact['id']}",
                    account_sha256=account_sha256,
                    binding_sha256=binding_sha256,
                    display_name=display_name,
                    effective_config=effective,
                ))
            elif ctype == 'group':
                group_id = contact.get('group_id')
                group_name = contact.get('group_name')
                if (not isinstance(group_id, str) or not group_id.strip()
                        or not isinstance(group_name, str) or not group_name.strip()):
                    continue
                read_mode = contact.get('group_name_read_mode') or 'auto'
                if read_mode not in GROUP_NAME_READ_MODES:
                    raise ValueError('invalid_group_name_read_mode')
                sidebar_width = contact.get('group_sidebar_width', 280)
                if type(sidebar_width) is not int or not 200 <= sidebar_width <= 600:
                    raise ValueError('invalid_group_sidebar_width')
                if not config.get('own_chat_anchor') or resolved_global_anchors['own_chat_anchor'] is None:
                    continue
                if config.get('account_anchor') and resolved_global_anchors['account_anchor'] is None:
                    continue
                g_anchor_raw = contact.get('group_anchor')
                raw_members = contact.get('group_member_anchors')
                if not g_anchor_raw or not isinstance(raw_members, list) or not 1 <= len(raw_members) <= 2:
                    continue
                try:
                    group_anchor_path = resolve_anchor_file(root_path, g_anchor_raw)[0]
                except ValueError as exc:
                    if str(exc) in {'invalid_anchor_path', 'unsafe_anchor_path'} or _is_anchor_symlink(root_path, g_anchor_raw):
                        raise
                    continue
                try:
                    resolved_members = []
                    seen_member_digests = set()
                    for raw_member in raw_members:
                        try:
                            member_path, member_digest = resolve_anchor_file(root_path, raw_member)
                        except ValueError as exc:
                            if str(exc) in {'invalid_anchor_path', 'unsafe_anchor_path'} or _is_anchor_symlink(root_path, raw_member):
                                raise
                            raise
                        if member_digest in seen_member_digests:
                            raise ValueError('duplicate_group_member_anchor')
                        seen_member_digests.add(member_digest)
                        resolved_members.append(member_path)
                    raw_name_anchor = contact.get('group_name_anchor')
                    resolved_name_anchor = None
                    if read_mode == 'anchor' or raw_name_anchor:
                        if not raw_name_anchor:
                            continue
                        try:
                            resolved_name_anchor = resolve_anchor_file(root_path, raw_name_anchor)[0]
                        except ValueError as exc:
                            if str(exc) in {'invalid_anchor_path', 'unsafe_anchor_path'} or _is_anchor_symlink(root_path, raw_name_anchor):
                                raise
                            raise
                except ValueError as exc:
                    if str(exc) in {'invalid_anchor_path', 'unsafe_anchor_path', 'duplicate_group_member_anchor'}:
                        raise
                    if any(_is_anchor_symlink(root_path, raw_m) for raw_m in raw_members) or (
                        raw_name_anchor and _is_anchor_symlink(root_path, raw_name_anchor)
                    ):
                        raise
                    continue
                binding_sha256 = compute_group_binding_sha256(root_path, config, contact)
                effective = dict(config)
                effective['group_id'] = group_id
                effective['group_name'] = group_name
                effective['group_query'] = (contact.get('group_query') or group_name).strip()
                effective['group_name_read_mode'] = read_mode
                effective['group_sidebar_width'] = sidebar_width
                effective['own_chat_anchor'] = resolved_global_anchors['own_chat_anchor']
                if resolved_global_anchors['account_anchor'] is not None:
                    effective['account_anchor'] = resolved_global_anchors['account_anchor']
                effective['group_anchor'] = group_anchor_path
                effective['group_member_anchors'] = resolved_members
                if resolved_name_anchor is not None:
                    effective['group_name_anchor'] = resolved_name_anchor
                # Routing and author pins come from the root-owned inbound policy.
                for key in (*GROUP_SCAN_ANCHOR_FIELDS, 'self_mention_names', 'registered_authors',
                            'sender_verify_mode', 'own_sender_refs', 'automated_sender_refs', 'at_all_tokens'):
                    effective.pop(key, None)
                if cfg_model.self_mention_names:
                    effective['self_mention_names'] = cfg_model.self_mention_names
                effective.update(_verified_group_scan_options(
                    root_path, cfg_model.group_inbound.get(contact['id'], {}), trusted_uid=trusted_uid,
                ))
                _apply_local_evidence_dirs(effective, root_path, 'group')
                display_name = (contact.get('display_name') or group_name).strip()
                targets.append(VerifiedScanTarget(
                    kind='group',
                    target_id=contact['id'],
                    target_key=f"group:{contact['id']}",
                    account_sha256=account_sha256,
                    binding_sha256=binding_sha256,
                    display_name=display_name,
                    effective_config=effective,
                ))
    elif not cfg_model.registered_own_posts:
        return {'state': 'disabled', 'reason': 'contacts_registry_missing', 'account_sha256': account_sha256, 'targets': ()}

    for own_post in cfg_model.registered_own_posts:
        if own_post.account_sha256 is not None and own_post.account_sha256 != account_sha256:
            raise ValueError('account_binding_changed')
        if not own_post.enabled:
            continue
        if config.get('account_anchor') and resolved_global_anchors['account_anchor'] is None:
            continue
        effective = dict(config)
        effective['own_post_id'] = own_post.own_post_id
        if resolved_global_anchors['account_anchor'] is not None:
            effective['account_anchor'] = resolved_global_anchors['account_anchor']
        _apply_local_evidence_dirs(effective, root_path, 'adhoc_moments')
        targets.append(VerifiedScanTarget(
            kind='own_post',
            target_id=own_post.own_post_id,
            target_key=f'own_post:{own_post.own_post_id}',
            account_sha256=account_sha256,
            binding_sha256=None,
            display_name=own_post.post_label or own_post.own_post_id,
            effective_config=effective,
        ))

    return {'state': 'ready', 'reason': None, 'account_sha256': account_sha256, 'targets': tuple(targets)}


def _validate_unread_hint(hint, *, expected_account_sha256, valid_target_keys, now_ms):
    if isinstance(hint, UnreadHintResult):
        raw = {
            'schema_version': hint.schema_version,
            'verified': hint.verified,
            'account_sha256': hint.account_sha256,
            'has_unread': hint.has_unread,
            'target_keys': hint.target_keys,
            'observed_at_ms': hint.observed_at_ms,
            'reason': hint.reason,
        }
    elif isinstance(hint, dict):
        raw = hint
    else:
        raise ValueError('invalid_unread_hint')

    if raw.get('schema_version') != INBOUND_SCAN_SCHEMA_VERSION:
        raise ValueError('invalid_unread_hint')
    if raw.get('verified') is not True:
        raise ValueError('unverified_unread_hint')
    if raw.get('account_sha256') != expected_account_sha256:
        raise ValueError('account_identity_mismatch')
    has_unread = raw.get('has_unread')
    if type(has_unread) is not bool:
        raise ValueError('invalid_unread_hint')
    observed_at_ms = raw.get('observed_at_ms')
    if observed_at_ms is not None:
        if type(observed_at_ms) is not int or not MIN_OBSERVED_MS <= observed_at_ms <= MAX_OBSERVED_MS:
            raise ValueError('invalid_unread_hint')
        if observed_at_ms > now_ms + HINT_FUTURE_SKEW_MS or now_ms - observed_at_ms > HINT_MAX_AGE_MS:
            raise ValueError('stale_unread_hint')
    raw_keys = raw.get('target_keys') or ()
    if not isinstance(raw_keys, (tuple, list)):
        raise ValueError('invalid_unread_hint')
    normalized_keys = []
    for key in raw_keys:
        if not isinstance(key, str) or key not in valid_target_keys:
            raise ValueError('unregistered_unread_hint_target')
        if key not in normalized_keys:
            normalized_keys.append(key)
    if not has_unread and normalized_keys:
        raise ValueError('inconsistent_unread_hint')
    reason = raw.get('reason')
    if reason is not None and not isinstance(reason, str):
        raise ValueError('invalid_unread_hint')
    return UnreadHintResult(
        schema_version=INBOUND_SCAN_SCHEMA_VERSION,
        verified=True,
        account_sha256=expected_account_sha256,
        has_unread=has_unread,
        target_keys=tuple(normalized_keys),
        observed_at_ms=observed_at_ms,
        reason=reason,
    )


def _validate_and_persist_slice_result(result, target, *, root, trusted_uid, clock):
    if isinstance(result, ScanSliceResult):
        slice_res = result
    elif isinstance(result, dict):
        allowed = {'target_key', 'observations', 'emitted_event_ids', 'ui_restored', 'draft_present', 'needs_review', 'reason'}
        if not set(result).issubset(allowed) or result.get('target_key') != target.target_key:
            raise ValueError('invalid_scan_slice_result')
        slice_res = ScanSliceResult(
            target_key=result['target_key'],
            observations=tuple(result.get('observations') or ()),
            emitted_event_ids=tuple(result.get('emitted_event_ids') or ()),
            ui_restored=bool(result.get('ui_restored', True)),
            draft_present=bool(result.get('draft_present', False)),
            needs_review=bool(result.get('needs_review', False)),
            reason=result.get('reason'),
        )
    else:
        raise ValueError('invalid_scan_slice_result')

    if slice_res.target_key != target.target_key:
        raise ValueError('scan_slice_target_mismatch')
    if not isinstance(slice_res.observations, (tuple, list)):
        raise ValueError('invalid_scan_slice_result')

    emitted_ids = list(slice_res.emitted_event_ids)
    if slice_res.observations:
        try:
            from gamma_cn.moments.inbox import InboundObservation, InboxStore, resolve_account_sha256
        except (ImportError, AttributeError) as exc:
            raise ValueError('gamma_inbound_scan_adapter_unavailable') from exc

        expected_kind = 'moments' if target.kind == 'own_post' else target.kind
        now_ms = int(clock() * 1000)
        container_prefix = CONTAINER_MOMENTS_PREFIX.rstrip('/') if str(root) == '/home/wechat/.gamma-moments' else None
        store = InboxStore(
            root,
            expected_uid=os.getuid(),
            allowed_parent_uids=frozenset({trusted_uid, os.getuid()}),
            container_prefix=container_prefix,
            receipts_dir=Path(root) / 'receipts',
        )
        for obs in slice_res.observations:
            if not isinstance(obs, InboundObservation):
                raise ValueError('invalid_inbound_observation')
            obs_acct = resolve_account_sha256(obs.account_sha256, obs.account_id)
            if obs_acct != target.account_sha256:
                raise ValueError('observation_account_mismatch')
            if obs.conversation_kind != expected_kind or obs.conversation_ref != target.target_id:
                raise ValueError('observation_target_mismatch')
            if target.kind in {'friend', 'group'}:
                if obs.binding_proof is not None:
                    if not obs.binding_proof.lock_verified or obs.binding_proof.binding_sha256 != target.binding_sha256:
                        raise ValueError('observation_binding_mismatch')
                elif not obs.needs_review and obs.trigger_type != 'needs_human':
                    raise ValueError('missing_observation_binding_proof')
            emit_res = store.record_observation(obs, clock_ms=now_ms, acquire_lock=False)
            if emit_res.event_id not in emitted_ids:
                emitted_ids.append(emit_res.event_id)

    return {
        'emitted_event_ids': tuple(emitted_ids),
        'ui_restored': bool(slice_res.ui_restored),
        'draft_present': bool(slice_res.draft_present),
        'needs_review': bool(slice_res.needs_review or slice_res.draft_present or not slice_res.ui_restored),
        'reason': slice_res.reason,
    }


class InboundScanScheduler:
    """Default-off staged single-target scheduler for future calibrated inbound scan adapters."""

    def __init__(self, scan_config=None):
        self.scan_config = (
            scan_config
            if isinstance(scan_config, InboundScanConfig)
            else InboundScanConfig.from_mapping(scan_config)
        )
        self.next_daily_mono = 0.0
        self.last_target_key = None
        self.last_scan_mono = None
        self.last_attempt_mono = None
        self.last_hint_mono = None
        self.outbound_deferred_since = None
        self.consecutive_outbound_ticks = 0
        self.consecutive_failures = 0

    def select_next_target(self, targets, *, hinted_keys=None):
        """Select exactly one verified target in fair cyclic order after the previous slice."""
        if not targets:
            raise ValueError('no_verified_targets')
        target_list = list(targets)
        keys = [t.target_key for t in target_list]
        if self.last_target_key in keys:
            start = (keys.index(self.last_target_key) + 1) % len(target_list)
        else:
            start = 0
        ordered = target_list[start:] + target_list[:start]
        if hinted_keys:
            hinted_set = set(hinted_keys)
            matched = [t for t in ordered if t.target_key in hinted_set]
            if matched:
                return matched[0]
        return ordered[0]

    def run_scan_slice(
        self,
        root,
        config,
        *,
        queue_class,
        adapter,
        clock=time.time,
        monotonic_clock=time.monotonic,
        trusted_uid=0,
        config_path=None,
        contacts_path=None,
        manual_reply_path=None,
        outbound_attempted=0,
    ):
        """Run at most one short, single-target inbound scan slice without holding locks across loops."""
        cfg = self.scan_config
        if cfg.enabled is not True:
            return {'state': 'disabled', 'reason': 'inbound_scan_disabled'}

        root = Path(root)
        try:
            resolved = resolve_inbound_scan_targets(
                root,
                config,
                trusted_uid=trusted_uid,
                config_path=config_path,
                contacts_path=contacts_path,
                manual_reply_path=manual_reply_path,
                scan_config=cfg,
            )
        except Exception as exc:
            return {'state': 'refused', 'reason': _safe_error_code(exc)}

        if resolved['state'] != 'ready':
            return {'state': 'disabled', 'reason': resolved['reason']}

        try:
            cap = validate_inbound_scan_adapter(adapter)
        except Exception as exc:
            return {'state': 'unavailable', 'reason': _safe_error_code(exc)}

        eligible = [t for t in resolved['targets'] if t.kind in cap['supported_target_kinds']]
        if not eligible:
            return {'state': 'idle', 'reason': 'no_supported_targets'}

        now_mono = float(monotonic_clock())
        if outbound_attempted > 0:
            if self.outbound_deferred_since is None:
                self.outbound_deferred_since = now_mono
            self.consecutive_outbound_ticks += 1
            fairness_due = (
                now_mono - self.outbound_deferred_since >= cfg.fairness_max_deferral_seconds
                or (
                    cfg.fairness_max_outbound_ticks is not None
                    and self.consecutive_outbound_ticks >= cfg.fairness_max_outbound_ticks
                )
            )
            if not fairness_due:
                return {'state': 'deferred', 'reason': 'outbound_priority'}
        else:
            self.outbound_deferred_since = None
            self.consecutive_outbound_ticks = 0
            if (self.last_attempt_mono is not None
                    and now_mono - self.last_attempt_mono < cfg.idle_scan_interval_seconds):
                return {'state': 'cooldown', 'reason': 'idle_interval'}

        account_sha256 = resolved['account_sha256']
        shared = queue_class(root / 'outbox', root / 'receipts')
        try:
            with shared.lock(account_sha256):
                try:
                    fresh_cfg = document(config_path) if config_path is not None else config
                    fresh_resolved = resolve_inbound_scan_targets(
                        root,
                        fresh_cfg,
                        trusted_uid=trusted_uid,
                        config_path=config_path,
                        contacts_path=contacts_path,
                        manual_reply_path=manual_reply_path,
                        scan_config=cfg,
                    )
                except Exception as exc:
                    self.last_attempt_mono = float(monotonic_clock())
                    return {'state': 'refused', 'reason': _safe_error_code(exc)}

                if fresh_resolved['state'] != 'ready':
                    return {'state': 'disabled', 'reason': fresh_resolved['reason']}
                if fresh_resolved['account_sha256'] != account_sha256:
                    return {'state': 'refused', 'reason': 'account_binding_changed'}
                fresh_targets = [t for t in fresh_resolved['targets'] if t.kind in cap['supported_target_kinds']]
                if not fresh_targets:
                    return {'state': 'idle', 'reason': 'no_supported_targets'}

                group_mention_ready = getattr(adapter, 'group_mention_ready', None)
                if len(fresh_targets) == 1 and fresh_targets[0].kind == 'group' and callable(group_mention_ready):
                    try:
                        ready = group_mention_ready(fresh_targets[0], root=root)
                        if type(ready) is not bool:
                            raise ValueError('invalid_group_mention_hint')
                    except Exception as exc:
                        self.last_attempt_mono = float(monotonic_clock())
                        return {'state': 'refused', 'reason': _safe_error_code(exc)}
                    if not ready:
                        self.last_attempt_mono = float(monotonic_clock())
                        return {'state': 'idle_hint_clear', 'reason': 'waiting_for_group_mention'}

                try:
                    actual_account = adapter.verify_account_identity(fresh_cfg)
                except Exception as exc:
                    self.last_attempt_mono = float(monotonic_clock())
                    return {'state': 'auth_required', 'reason': _safe_error_code(exc)}
                if actual_account != account_sha256:
                    self.last_attempt_mono = float(monotonic_clock())
                    return {'state': 'auth_required', 'reason': 'account_not_verified'}

                lock_mono = float(monotonic_clock())
                fallback_due = (
                    self.last_scan_mono is None
                    or lock_mono - self.last_scan_mono >= cfg.fallback_interval_seconds
                )
                hinted_keys = None
                if not fallback_due and cfg.use_unread_hint and cap['supports_unread_hint']:
                    try:
                        raw_hint = adapter.read_unread_hint(
                            account_sha256=account_sha256,
                            targets=fresh_targets,
                            deadline_monotonic=lock_mono + cfg.slice_budget_seconds,
                            config=fresh_cfg,
                        )
                        hint = _validate_unread_hint(
                            raw_hint,
                            expected_account_sha256=account_sha256,
                            valid_target_keys={t.target_key for t in fresh_targets},
                            now_ms=int(clock() * 1000),
                        )
                    except Exception as exc:
                        self.last_attempt_mono = float(monotonic_clock())
                        return {'state': 'refused', 'reason': _safe_error_code(exc)}
                    if not hint.has_unread:
                        self.last_attempt_mono = float(monotonic_clock())
                        self.last_hint_mono = self.last_attempt_mono
                        return {'state': 'idle_hint_clear', 'reason': 'no_unread_hint'}
                    if hint.target_keys:
                        hinted_keys = hint.target_keys

                selected = self.select_next_target(fresh_targets, hinted_keys=hinted_keys)
                slice_start_mono = float(monotonic_clock())
                self.last_attempt_mono = slice_start_mono
                self.outbound_deferred_since = None
                self.consecutive_outbound_ticks = 0

                deadline = slice_start_mono + cfg.slice_budget_seconds
                slice_error = None
                persisted = None
                restore_result = None
                restore_error = None
                try:
                    raw_result = adapter.scan_target(
                        selected,
                        deadline_monotonic=deadline,
                        slice_budget_seconds=cfg.slice_budget_seconds,
                        root=root,
                        config=fresh_cfg,
                    )
                    persisted = _validate_and_persist_slice_result(
                        raw_result,
                        selected,
                        root=root,
                        trusted_uid=trusted_uid,
                        clock=clock,
                    )
                except Exception as exc:
                    slice_error = exc
                finally:
                    try:
                        restore_result = adapter.restore_ui(selected)
                    except Exception as exc:
                        restore_error = exc

                end_mono = float(monotonic_clock())
                self.last_attempt_mono = end_mono
                self.last_target_key = selected.target_key
                elapsed_seconds = max(0.0, end_mono - slice_start_mono)
                budget_exceeded = end_mono > deadline
                restore_failed = (
                    restore_error is not None
                    or restore_result is False
                    or (isinstance(restore_result, dict) and restore_result.get('ui_restored') is False)
                )
                draft_present = bool(
                    (persisted and persisted['draft_present'])
                    or (isinstance(restore_result, dict) and restore_result.get('draft_present') is True)
                )
                if restore_failed or draft_present or (persisted and persisted['needs_review']):
                    self.consecutive_failures += 1
                    reason = (
                        'existing_draft_requires_review'
                        if draft_present
                        else ('ui_restore_failed' if restore_failed else (persisted['reason'] or 'scan_needs_review'))
                    )
                    if restore_error is not None and not draft_present:
                        reason = _safe_error_code(restore_error)
                    return {
                        'state': 'needs_review',
                        'target_key': selected.target_key,
                        'target_kind': selected.kind,
                        'target_id': selected.target_id,
                        'ui_restored': not restore_failed and (persisted['ui_restored'] if persisted else True),
                        'draft_present': draft_present,
                        'budget_exceeded': budget_exceeded,
                        'elapsed_seconds': elapsed_seconds,
                        'emitted_event_ids': persisted['emitted_event_ids'] if persisted else (),
                        'reason': reason,
                        'scan_error': _safe_error_code(slice_error) if slice_error else None,
                    }

                if slice_error is not None:
                    self.consecutive_failures += 1
                    return {
                        'state': 'failed',
                        'target_key': selected.target_key,
                        'target_kind': selected.kind,
                        'target_id': selected.target_id,
                        'ui_restored': True,
                        'budget_exceeded': budget_exceeded,
                        'elapsed_seconds': elapsed_seconds,
                        'reason': _safe_error_code(slice_error),
                    }

                self.last_scan_mono = end_mono
                self.consecutive_failures = 0
                return {
                    'state': 'scanned',
                    'target_key': selected.target_key,
                    'target_kind': selected.kind,
                    'target_id': selected.target_id,
                    'fallback': fallback_due,
                    'budget_exceeded': budget_exceeded,
                    'elapsed_seconds': elapsed_seconds,
                    'emitted_event_ids': persisted['emitted_event_ids'],
                    'ui_restored': True,
                    'draft_present': False,
                }
        except BlockingIOError:
            return {'state': 'busy', 'reason': 'account_lock_busy'}


def _is_adhoc_job_pending(job_path, receipts_dir):
    receipt_path = Path(receipts_dir) / (Path(job_path).stem + '.json')
    if not receipt_path.exists() and not receipt_path.is_symlink():
        return True
    try:
        receipt = document(receipt_path)
    except Exception:
        return True
    return receipt.get('state') not in TERMINAL_RECEIPT_STATES


def run_worker_iteration(
    root,
    config_path,
    *,
    scheduler,
    queue_class,
    run_one_fn,
    desktop_class,
    tick_fn=None,
    clock=time.time,
    monotonic_clock=time.monotonic,
    trusted_uid=0,
    friend_label_anchor=None,
    contacts_path=None,
    manual_reply_path=None,
    group_desktop_class=None,
    adhoc_desktop_class=None,
    inbound_scan_adapter=None,
    stopping=None,
):
    """Run one complete worker turn: daily check, once-per-pending-item outbox, and bounded scan."""
    root = Path(root)
    config_path = Path(config_path)
    now_mono = float(monotonic_clock())

    daily_status = None
    if tick_fn is not None and now_mono >= scheduler.next_daily_mono:
        try:
            daily_status = tick_fn(root, config_path)
        except Exception as error:
            daily_status = {'state': 'worker_error', 'error_type': type(error).__name__}
            print(json.dumps({'worker_error': type(error).__name__}), flush=True)
        scheduler.next_daily_mono = now_mono + 60

    try:
        config = document(config_path)
    except Exception as error:
        print(json.dumps({'worker_error': type(error).__name__}), flush=True)
        return {
            'daily': daily_status,
            'outbox': [],
            'scan': {'state': 'refused', 'reason': _safe_error_code(error)},
            'new_outbox_pending': False,
        }

    outbox_dir = root / 'adhoc' / 'outbox'
    receipts_dir = root / 'adhoc' / 'receipts'
    outbox_results = []
    attempted_stems = set()
    for path in sorted(outbox_dir.glob('*.json')):
        if stopping is not None and stopping.is_set():
            break
        if not _is_adhoc_job_pending(path, receipts_dir):
            continue
        attempted_stems.add(path.stem)
        try:
            res = process_job(
                path, root, config, queue_class, run_one_fn, desktop_class,
                clock=clock, trusted_uid=trusted_uid,
                friend_label_anchor=friend_label_anchor, config_path=config_path,
                contacts_path=contacts_path, group_desktop_class=group_desktop_class,
                adhoc_desktop_class=adhoc_desktop_class, manual_reply_path=manual_reply_path,
            )
            outbox_results.append({'job_id': path.stem, 'state': res.get('state'), 'reason': res.get('reason')})
        except Exception as error:
            outbox_results.append({'job_id': path.stem, 'state': 'error', 'reason': type(error).__name__})
            print(json.dumps({'worker_error': type(error).__name__}), flush=True)

    if stopping is not None and stopping.is_set():
        scan_result = {'state': 'stopping'}
    else:
        scan_result = scheduler.run_scan_slice(
            root,
            config,
            queue_class=queue_class,
            adapter=inbound_scan_adapter,
            clock=clock,
            monotonic_clock=monotonic_clock,
            trusted_uid=trusted_uid,
            config_path=config_path,
            contacts_path=contacts_path,
            manual_reply_path=manual_reply_path,
            outbound_attempted=len(attempted_stems),
        )

    new_outbox_pending = any(
        p.stem not in attempted_stems and _is_adhoc_job_pending(p, receipts_dir)
        for p in sorted(outbox_dir.glob('*.json'))
    )
    return {
        'daily': daily_status,
        'outbox': outbox_results,
        'scan': scan_result,
        'new_outbox_pending': new_outbox_pending,
    }


def main(
    argv=None,
    *,
    stopping=None,
    max_iterations=None,
    inbound_scan_config=None,
    inbound_scan_adapter=None,
    trusted_uid=0,
    clock=time.time,
    monotonic_clock=time.monotonic,
    tick_fn=None,
    queue_class=None,
    run_one_fn=None,
    desktop_class=None,
    group_desktop_class=None,
    adhoc_desktop_class=None,
    manual_reply_path=None,
):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--contacts-config', type=Path, default=None)
    parser.add_argument('--inbound-scan-config', type=Path)
    parser.add_argument('--legacy-module', choices=['gamma_cn.moments.worker'], required=True)
    parser.add_argument('--poll-seconds', type=int, required=True)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--friend-label-anchor', type=Path)
    args = parser.parse_args(argv)
    if not 2 <= args.poll_seconds <= 30:
        raise ValueError('invalid_poll_interval')

    if tick_fn is None:
        from gamma_cn.moments.worker import tick as gamma_tick
        tick_fn = gamma_tick
    if queue_class is None or run_one_fn is None:
        from gamma_cn.moments.delivery import Queue as gamma_queue, run_one as gamma_run_one, atomic_json
        queue_class = queue_class or gamma_queue
        run_one_fn = run_one_fn or gamma_run_one
    else:
        from gamma_cn.moments.delivery import atomic_json
    if desktop_class is None:
        from gamma_cn.moments.direct import DirectDesktop as gamma_direct_desktop
        desktop_class = gamma_direct_desktop

    probed_group_cls, probed_adhoc_cls, task2_available = _probe_optional_task2_adapters()
    group_cls = probed_group_cls if group_desktop_class is None else group_desktop_class
    adhoc_cls = probed_adhoc_cls if adhoc_desktop_class is None else adhoc_desktop_class
    if group_desktop_class is not None or adhoc_desktop_class is not None:
        task2_available = group_cls is not False and adhoc_cls is not False

    if args.inbound_scan_config is not None and inbound_scan_config is not None:
        raise ValueError('duplicate_inbound_scan_config')

    def read_scan_config():
        if args.inbound_scan_config is not None:
            raw = _read_trusted_worker_json(
                args.inbound_scan_config, args.root, trusted_uid, 'untrusted_inbound_scan_config',
            )
            return InboundScanConfig.from_mapping(raw)
        if isinstance(inbound_scan_config, InboundScanConfig):
            return inbound_scan_config
        return InboundScanConfig.from_mapping(inbound_scan_config)

    scan_cfg = read_scan_config()
    if inbound_scan_adapter is not None:
        try:
            validate_inbound_scan_adapter(inbound_scan_adapter)
            scan_adapter = inbound_scan_adapter
            scan_adapter_available = True
        except ValueError:
            scan_adapter = None
            scan_adapter_available = False
    else:
        scan_adapter, scan_adapter_available = _probe_optional_inbound_scan_adapter()

    startup_cfg = document(args.config)
    if _task2_enabled_in_runtime(
        args.root, startup_cfg, contacts_path=args.contacts_config, config_path=args.config, trusted_uid=trusted_uid,
    ) and not task2_available:
        raise RuntimeError('gamma_task2_adapter_unavailable')
    if scan_cfg.enabled and not scan_adapter_available:
        raise RuntimeError('gamma_inbound_scan_adapter_unavailable')

    queue = queue_class(args.root / 'outbox', args.root / 'receipts')
    if args.dry_run:
        jobs = list((args.root / 'adhoc' / 'outbox').glob('*.json'))
        for path in jobs:
            validate_job(
                path, startup_cfg, int(clock() * 1000), trusted_uid=trusted_uid,
                root=args.root, contacts_path=args.contacts_config, config_path=args.config,
                manual_reply_path=manual_reply_path,
            )
        print(json.dumps({'validated': len(jobs), 'submitted': 0}))
        return

    if args.once and max_iterations is None:
        max_iterations = 1
    stop_event = stopping if stopping is not None else threading.Event()
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
        signal.signal(signal.SIGINT, lambda *_: stop_event.set())

    scheduler = InboundScanScheduler(scan_cfg)
    pid_path = args.root / 'receipts' / 'worker.pid.json'
    with queue.lock(sha('moments-daemon')):
        atomic_json(pid_path, {'pid': os.getpid()})
        iterations = 0
        scan_config_error = None
        last_scan_issue = None
        try:
            while not stop_event.is_set():
                if args.inbound_scan_config is not None:
                    try:
                        latest_scan_cfg = read_scan_config()
                        if latest_scan_cfg.enabled and not scan_adapter_available:
                            raise ValueError('gamma_inbound_scan_adapter_unavailable')
                    except ValueError as error:
                        reason = _safe_error_code(error)
                        if reason != scan_config_error:
                            print(json.dumps({'inbound_scan_error': reason}), flush=True)
                        scan_config_error = reason
                        latest_scan_cfg = InboundScanConfig()
                    else:
                        scan_config_error = None
                    scheduler.scan_config = latest_scan_cfg
                turn = run_worker_iteration(
                    args.root,
                    args.config,
                    scheduler=scheduler,
                    queue_class=queue_class,
                    run_one_fn=run_one_fn,
                    desktop_class=desktop_class,
                    tick_fn=tick_fn,
                    clock=clock,
                    monotonic_clock=monotonic_clock,
                    trusted_uid=trusted_uid,
                    friend_label_anchor=args.friend_label_anchor,
                    contacts_path=args.contacts_config,
                    manual_reply_path=manual_reply_path,
                    group_desktop_class=group_cls,
                    adhoc_desktop_class=adhoc_cls,
                    inbound_scan_adapter=scan_adapter,
                    stopping=stop_event,
                )
                scan = turn['scan']
                if scan['state'] in {'auth_required', 'needs_review', 'refused', 'unavailable', 'failed'}:
                    reason = _safe_error_code(scan.get('reason', 'scan_failed'))
                    error = scan.get('scan_error')
                    issue = (scan['state'], reason, error)
                    if issue != last_scan_issue:
                        print(json.dumps({
                            'inbound_scan_state': scan['state'],
                            'reason': reason,
                            'scan_error': error,
                        }), flush=True)
                    last_scan_issue = issue
                elif scan['state'] == 'scanned':
                    last_scan_issue = None
                iterations += 1
                if max_iterations is not None and iterations >= max_iterations:
                    break
                if turn['new_outbox_pending']:
                    continue
                stop_event.wait(args.poll_seconds)
        finally:
            pid_path.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
