# Linux 运维工具

[English](README.md) | 中文

## 概述

这些可选服务端工具将受沙箱约束的 DSH Agent 接入操作范围固定的 Unix socket 运维服务，以及现有 Gamma 微信桌面 worker。它们不进入桌面默认启动组合或 DMG，由管理员在 Linux 主机上单独安装；模型不会获得 sudo 或通用 Podman 权限。

## 指令与权限

使用 host_status、service_status、service_logs、wechat_status 和 wechat_screenshot 进行检查。服务别名、可重启容器与已登记的微信好友由管理员配置。worker 的 ready 心跳只是界面观察，不证明网络投递成功。

好友直发使用 `wechat_send`，已登记群聊使用 `wechat_send_group`，只读群身份核验使用 `wechat_probe_group`（共享账号锁但不输入或点击发送），按需朋友圈在管理员开启开关后使用 `wechat_publish_moment`。认证只证明说话人身份，broker 还会核验当前真实飞书消息到底授权发送什么内容：只有当整条可信入站消息无歧义地匹配直接发送或发布句式（例如“给<对象>发微信：<正文>”、“给<群>发群消息：<正文>”、“发[仅自己可见/好友可见]朋友圈：<正文>”），并与冻结的目标、逐字正文及可见范围一致（仅当管理员配置了默认可见范围且原消息未另行指定时方可使用默认值，否则必须明确写出私密或好友可见），且不含草稿、否定、引用、假设、询问或状态检查语境时，才会获得当回合 `direct=true` 授权。仅起草、先别发、对话转述、正文替换、可见范围越权升级以及升级前遗留的旧 `direct=true` 记录均绝不会直接发送，`prepare` 只返回 `direct=false` 的冻结预览并要求本人在后续回合明确回复 `确认执行 <id>`；同一请求回合内绝不允许多个或变体直接发送，而两次独立认证的本人请求（即便正文完全相同）仍各自获得独立的 24 位 hex 任务编号与 `{job_id}.{text_sha256[:12]}.png` 证据文件。好友和群聊通过稳定不透明 ID、唯一别名以及无歧义的精确显示名或群名指定；显示名重名、未绑定、已停用、未知目标、好友/群类型混用和目标替换都会被拒绝。按需朋友圈拥有在生产默认关闭的独立开关，严格校验 220 非空白字符、4 行、保留字与 620x800 视口限制且绝不改写原文本，与每日朋友圈的去重和限额完全隔离。旧单收件人配置和升级前已入队任务仍只绑定其原联系人。服务及限额修改仍保留编号确认，严格格式的重启指令除外。所有操作继续保留去重及不确定结果核实。

set_limits 只修改 logLines 和 previewSeconds，并持久保存备份。restore_limits 在重新确认后恢复已完成修改，配置在预览后发生变化则拒绝执行。不提供任意命令、路径、微信号、群标识、凭据编辑或目标白名单修改。

微信任务在获得 worker 回执前只是排队状态。sent_verified 与 published_verified 仅表示已核对发送端界面及完整正文，不代表远端投递或对方已读；wechat_probe_group 返回 identity_verified 表示群详情界面、1–2 个成员头像锚点及精确群名核验通过。执行结果不确定时转为 needs_review，绝不自动重发。适配入口保留原每日 tick、daemon 锁和账号锁，向 Gamma `delivery.run_one` 移交临时任务时不加重复外层锁，且临时消息使用独立的 outbox 与回执空间。

## 安装与测试

使用 npm ci --prefix scripts/linux-operations --ignore-scripts --no-audit --no-fund 安装独立且锁定版本的 MCP 适配器依赖，使用 pnpm run test:linux-operations 执行 Node 测试，使用 python3 -m unittest scripts/linux-operations/tests/test_install_release.py -v 执行发布清单与安装器隔离测试。完整 Python worker 测试要求 PYTHONPATH 指向真实 Gamma 源码，运行 python3 -m unittest discover -s scripts/linux-operations/tests -p 'test_*.py' -v；只替换桌面，不替换原队列事务。

install.py 仅供管理员在现有 Octopus/Gamma 主机布局安装，并保持对宿主机 Python 3.6.8+ 标准库与语法的兼容（同时支持容器内的 Python 3.12+ `wechat_worker.py`）：--source、--runtime 和 --node 必须显式提供绝对路径，可用 --contacts-config 指定好友和群聊注册表，可用 --adhoc-moments-config 配置按需朋友圈开关（生产默认关闭），并可用 --autoreply-config 显式启用受限 DSH 自动回复策略（严格默认关闭）。`RELEASE_FILES` 已完整包含全部运行时模块（`broker.mjs`、`operations.mjs`、`policy.mjs`、`mcp-server.mjs`、`readiness.mjs`、`wechat-inbox.mjs`、`wechat-autoreply.mjs`、`wechat-autoreply-dsh.mjs`、`wechat_worker.py`、`install.py`）。阶段 A 会在停止活动服务或切换符号链接前拒绝不可在生产独立启动的测试传输 `transport.mode: 'adapter'`（包含新传参和既有 `operations.json` 中残留配置），强制要求 `deepseek-api-key` 必须通过规范化的归 root 所有 `0600`/`0400` 权限 `environmentFile`（校验全链路无组/其他可写上级目录、非 root 符号链接上级、`-` 可选前缀、通配符或 `%` systemd 占位符，同时兼容如 `/var/run -> /run` 等 root 发行版符号链接）或既有 `octopus-operations.service` / `.d/*.conf` drop-in `EnvironmentFile=` 提供非空专用凭据（绝不依赖不会被 systemd 继承的登录 shell 环境变量），并将清单文件拷贝到临时隔离符号链接目录离线验证本地导入、策略校验、Gamma 适配模块（启用时）、飞书 `ownerNotices` 能力（启用时）与 DSH `resolveDshInstallAnchor`/`loadDshRuntimeModules`，在自动回复启用时还会使用上述 `EnvironmentFile` 构造隔离服务环境真实启动一次 `current/broker.mjs` 并完成 Unix socket 握手且绝不发起外部模型请求。启用 `--autoreply-config` 时拒绝任何内嵌密钥字段或未获显式授权的 `DEEPSEEK_API_KEY` 回退，仅将校验后的规范 `environmentFile` 路径写入 `octopus-operations.service` 的 `EnvironmentFile=`。安装器会将 DSH 注册表写入 Gamma config/config.json 旁归 root 所有的独立 config/octopus-contacts.json 附表，由 Gamma 继续保有原文件、活动账号绑定及每日开关，同时保留原 Gamma 源码与配置备份；将 worker 入口替换为在界面提交前复核联系人或群锚点与按需朋友圈策略的适配入口，不重启微信登录容器；刷新飞书入口前等待当前人工任务完成。

凭据保留在原有仅 root 可读的环境文件中，不复制到这些工具。broker socket 目录归 root 所有，组为专用 Agent 组，操作日志仅 root 可读。有限日志会脱敏常见凭据赋值和 Bearer 字符串；管理员仍应避免在日志中输出秘密。

好友名片的 OCR 兼容处理要求在现有微信交换目录的 config/octopus-friend-label.png 保存管理员核验且归 root 所有的锚点，且每位已绑定好友都需各自归 root 所有且非符号链接的搜索、侧栏和名片锚点；已绑定群聊则要求 `own_chat_anchor`、唯一 `group_anchor`、1–2 个互不重复的 `group_member_anchors`，以及原生剪贴板或 `group_name_anchor` 精确群名核验，且群聊绝不注入好友名片标签兼容逻辑；缺少、重复、符号链接或可被其他用户修改的锚点会拒绝安装。

## 微信到飞书通知与 v1 生产者契约

`wechat-inbox.mjs`（运行于 `broker.mjs` 内的 `WechatExchangeProjector`）与 `@deepseek-ai/dsh-experimental-im-feishu`（运行于 `ingress-plugin.ts` 内的 `OwnerNoticeStorage`、`OwnerNoticeDispatcher` 与 `OwnerNoticeHandoffService`）共同组成默认关闭、显式开启（`wechatNotifications.enabled: false` 与 `ownerNotices.enabled: false`）的两段式通知链路。root 权限的 broker 按固定生产者 UID，通过 `O_RDONLY | O_NOFOLLOW` 与打开描述符 `fstatSync(fd)` 读取 `wechatRoot`，将每条 `adhoc/receipts/*.json` 与 `adhoc/outbox/*.json` 及 `operations.sqlite` 中的已冻结任务交叉核验后在私有 `wechat-inbox.sqlite` 完成去重聚合，再仅向受限的 `root:octopus-ingress`（`0750`/`0640`）移交目录写入最小脱敏摘要，既不放宽既有目录与文件权限（`wechatRoot` `0750`、`adhoc/outbox` `2750`、`adhoc/receipts` `0700`/`0600`），也不让 `octopus-ingress` 加入 `1000` 特权组或触碰原始路径。

当启用 `ownerNotices.enabled` 且存在活动的飞书通道时，`ingress-plugin.ts` 会在已有 `ImWriterLease` 下开启独立的 `owner-notices.sqlite`（`im_owner_notice_notices` 与 `im_owner_notice_threads`），启动时将残留的 `in_flight` 记录收敛为 `unknown`，在独立于普通 IM 回复循环的非重叠 `noticePump` 内按受限每轮上限（`maxIngestPerCycle` 默认 `20`、`maxSendsPerCycle` 默认 `5`）读取移交摘要并通过共用的 `FeishuTransport.sendTextMessage` 与当前未吊销的 `u_im_ingress_operators` 绑定下发，收到停机信号后不再认领新通知并仅在 `shutdownTimeoutMs` 内等待已认领请求收尾，所有外部文本强制标记 `untrusted: true`、`authority: 'notice_only'`，且绝不写入 `u_im_ingress_deliveries`；缺少飞书通道或绑定失效时直接拒绝。

启用 broker 侧 `wechatNotifications.enabled` 后，入站通知按会话独立维护默认 20 秒的滑动聚合窗口（可在 15–30 秒配置，并受 `inboundMaxBatchWaitSeconds` 与 `inboundMaxEventsPerBatch` 封顶；`needs_human`/`needs_review` 立即触发），每批共享一个 `wn_<12hex>` `noticeId` 并自动复用同会话线程 `replyMessageId`，维护有界上下文与保留策略（`maxContextMessages`、`retentionDays`），同时仅对 `sent_verified`、`published_verified`、`identity_verified`、`needs_review`、`failed_before_submit`、`refused`、`expired` 和 `auth_required` 发送带非远端送达声明的异步终态回执，并在 `projected_notices` 持久记录 `'pending' -> 'written'` 移交状态以在写盘失败或崩溃重启后按确定性编号有界补写缺失移交文件而不重复发或重放健康/'unknown'回执；`install.py` 已将 `wechat-inbox.mjs` 纳入 `RELEASE_FILES` 并支持 `--notifications-config` 与离线已构建包 `--feishu-package`。按通知编号人工回复（`wechat_reply_notice`，受独立默认关闭开关 `wechatNotifications.manualReplyEnabled` 与有效期 `manualReplyMaxAgeSeconds` 默认 `3600` 秒约束）只接收 `{noticeId, text}` 而不接受自由目标字段，由 broker 从 `wechat-inbox.sqlite` 只读解析具有观测期显式 `binding_sha256` 与冻结入站绑定的好友/群聊 `inbound_alert`（缺少或为空/旧版推断入站绑定的记录、回执通知、朋友圈动态、需人工核查或来源不明通知、以及已换绑或停用目标一律拒绝），按同回合直接回复句式（`回复 wn_<12hex>：<正文>`）绑定逐字正文，在准备与排队阶段将截止时间收敛为操作有效期与基于 `source_observed_at`/`created_at`/有限租约计算的当前期限中的较早值且绝不延长已冻结截止时间，随 `origin_notice_id` 一并下发 `origin_notice_key`、`origin_notice_observed_at`、`origin_notice_created_at`，并以归 root 所有的 `config/octopus-manual-reply.json`（`0o640`）作为单一运行时暂停/租约状态真源：broker 启动（`startBroker`）、轮询刷新（`createBrokerProjectionLoop`）与常规 `install.py` 重装仅执行单调收紧同步（`reconcileManualReplyConfigView`：绝不合成缺失或不可读文件，绝不清除 `paused`、有限 `lease_expires_at` 或缩短后的 `max_age_seconds`，且策略关闭或账号换绑继续失败关闭）。运维人员由 root 显式执行 `node <current>/broker.mjs <policyPath> manual-reply <status|pause|resume> [--lease-seconds N] [--max-age-seconds N]`（可在不放宽 worker/ingress 权限及不访问 broker SQLite 的前提下即时生效并拦截尚未送审的已排队任务）或使用 `install.py --resume-manual-reply` / `--pause-manual-reply`（可选 `--manual-reply-lease-seconds N`）完成暂停、恢复或限时租约延期，且不影响常规回执通知与普通消息发送。无论新开还是保留通知启用状态，`install.py` 阶段 A 都会按与 `@deepseek-ai/dsh-app-boot` 相同的解析顺序（`profile/package.json` + `profile/node_modules` 本地优先于 runtime 依赖图与 profile 回退）实载检查 `@deepseek-ai/dsh-experimental-im-feishu/ingress` 导出的 `ownerNoticesCapability` 与配置结构，在停服或改动配置前拒绝 `.ts` 源码或缺少能力声明的旧包，将已校验的 `--feishu-package` 构建物归档至 `release_root/feishu-packages/<digest>` 并实装入 `profile/node_modules` 同时固定依赖版本，并在入口重启失败时完整回滚 `profile/package.json`、`node_modules`、`cordis.patch.yml` 与 `config/octopus-manual-reply.json`。

上游桌面采集程序可在与 `adhoc/receipts` 相同的固定 UID（生产 UID `1000`）下，将每条入站事件写成常规、非符号链接、非组可写与非全局可访问（`0600`/`0640`，上限 32 KiB）的 `wechatRoot/inbox/events/<event_id>.json` 对象（Wire Schema v1）。必填字段包括 `schema_version`（`1`）、与文件名一致的 `event_id`、当前绑定账号的 64 位 hex `account_sha256`、`trigger_type`（`friend_message` | `group_message` | `moments_interaction` | `needs_human`）、`conversation_kind`（`friend` | `group` | `moments`）、`conversation_ref`（好友/群填已绑定 `contact.id`，朋友圈填安全标识）、`sender_name`、`source_ref`、`source_time`（不可得时填 `"unknown"`）、合法时间窗内的毫秒级 `observed_at`、非空 `text`（不超过 4096 UTF-8 字节）及受控目录内校验过哈希的 `evidence_path` + `evidence_sha256`（仅 `needs_human` 允许两者同为 `null`）；可选字段仅限 `conversation_name`、`sender_ref`、`quoted_sender`、`quoted_text`、`needs_review`、`reason` 与 `binding_sha256`（需显式携带与已绑定好友/群目标匹配的观测期 64 位 hex `binding_sha256` 方可具备人工回复资格；缺失或为 `null` 的合法事件仅作为只读通知投影（`reply_eligible = 0`），而显式提供但不匹配的 `binding_sha256` 在入库阶段即失败关闭拒绝）。

通过 `--inbound-scan-config` 提交管理员控制的 JSON 配置以启用桌面收件扫描。安装器把配置保存为联系人注册表旁的 `octopus-inbound-scan.json`。显式设置 `enabled` 并登记扫描对象。群聊使用 `self_mention_names` 和逐群 `group_inbound` 设置核验作者和 @ 锚点。已核验的本人帖子使用 `registered_own_posts`。worker 持有共享账号锁时每次扫描一个目标，游标随已落盘的入站事件推进。

通过认证的飞书操作者可以发送「看刚发的朋友圈评论」或「看<登记对象>最近消息」。每次最多返回八条已采集记录，按登记会话或最近采集过互动的本人帖子查询。

## 自动回复策略与投递

自动回复默认关闭。通过 `--autoreply-config` 配置专用传输并逐会话选模式。`draft_only` 在开启 `projectOwnerNotices` 后向操作者推送拟稿。`unattended` 还要求原生发件人证据和匹配的新鲜会话尾部。群聊 @ 需要可靠原生印证，引用需要核验被引用原消息。@所有人仅提醒。限流按群计算。

受限 DSH profile 生成回复正文或转人工。broker 把一次决定和一个目标冻结入账。桌面 worker 持有账号锁时核对短期管理员许可，并重读原会话尾部。发现本人已回复或对方追发就停止提交。提交结果不明时保留台账待核对。飞书自动回复回执列出对方原话、回复正文和发送端终态。

启用无人值守前，在目标账号验收原生发件人和尾部核对、时钟与手机同步、已安装凭据及真实飞书投递。Linux 朋友圈适配器可读取评论并生成互动事件。指定评论回评仍需要核验原生挂接控件。

## 恢复与验收

从 profile 移除工具前先停止入口。恢复已备份的 profile 并移除 worker 的 systemd drop-in，即可回到原 worker。检查活动投递后再重启 worker、broker 和入口，保留不确定回执用于核实；不得清空回执强制重试。

本地假桌面测试不证明真实微信发送。生产验收另行检查 root/组权限、安装后的 MCP 协商、真实模型只读调用、账号与联系人身份预检，以及本人授权的真实消息。安装成功本身不会向生产联系人发送测试消息。
