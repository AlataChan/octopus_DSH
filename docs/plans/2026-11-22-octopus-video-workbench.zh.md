# octopus_DSH 视频工作台集成方案

[English](2026-11-22-octopus-video-workbench.md) | 中文

**状态**　第二轮评审稿。四项首发决策已纳入。阶段 0 待批准启动，阶段 1/2 以各自的实测证据放行。

## 用户流程

从首页「专业能力 → 视频制作」进入，也可在普通会话发起。导入照片、视频、真实页面截图、旁白、配乐或 SRT 后，Agent 给出镜头顺序、时长和素材来源。用户可预览、导出 MP4，重开应用后继续修改。作品列表保留当前修订、导出进度和历次成功成片。基础版用一条 30–45 秒的公益活动回顾验收。专业版另用真实产品界面截图做一条演示，验证 Shotcraft 的镜头与模板。

## 首发决策与交付阶段

| 决策 | 采用范围 |
| --- | --- |
| 两档交付 | 基础成片优先交付可继续修改的视频项目。专业版按需开启「镜头配方」。 |
| 供应商 | 国内 MiniMax 按量账户用于默认视频、配音和待验的 Image-01。Seedance 2.0 独立开通、配置与验证，产出视频。仅在 Image-01 未通过阶段 0 时另评审 Seedream。 |
| 平台与专业渲染 | 首发 macOS arm64。专业运行时由用户按需在本地安装。DMG 不包含 FFmpeg、Remotion 或 Chromium。托管渲染留待后续版本。 |
| 微信 | v1 通过文件与截图导入素材。个人微信配置、只读采集和消息触发分别留待后续评审。 |

| 阶段 | 实现与范围 | 放行依据 |
| --- | --- | --- |
| 0 · 核验前置条件 | 冻结版本与素材来源。按下表核验干净环境、授权、原生预览及供应商。 | 为基础能力、每条供应商路线和专业运行时分别记录结果与证据。 |
| 1 · 基础成片 | 在媒体 overlay 增加可修订工程、镜头、旁白、配乐、SRT、横竖版和本地导出。生成选项按各自证据开放。 | 从正常桌面入口完成出片，重启后可继续改。改配乐调用数和费用增量都为零。只换一段旁白时，仅重算引用它的步骤。 |
| 2 · 专业镜头 | 使用固定版本的 Shotcraft 卡片及对应 demo、逐项授权的素材和一个可编辑模板。支持模板、共同创作、自主创作及单镜头。 | 同一工程可改镜头参数、预览和导出。验证真实截图、多轨、中文字体、关键帧与实际听感。 |

阶段 1 开工前须有干净环境的基础本地流程与权限验证，以及该阶段素材和分发方式的许可依据。附加生成能力单独放行，失败时保留素材导入和本地编辑。阶段 2 还须通过专业渲染、预览与授权核验。阶段 0 用现有安装版与最小验证工程取得前置证据，可先做无付费、无真实用户素材的验证。新增工作台功能的端到端验收在阶段 1/2 执行。

## 工程存储、修订与预览

从 MoneyPrinterTurbo 借鉴不可变修订、稳定镜头 ID、按依赖指纹和实际 SHA-256 复用产物。复用工程设计，沿用 DSH 的任务与媒体运行时。工程记录素材 ID、轨道、时间、输入摘要、模板与渲染器版本、参数和导出。重新排序时保留镜头 ID。画面、旁白、字幕、拼接与混音按依赖重算。全局字幕样式变化只影响引用它的输出。 [MoneyPrinterTurbo 项目模式](https://github.com/harry0703/MoneyPrinterTurbo/blob/86e45a2233a72c41d83389ac5cb49bbf5f6a847d/docs/video-projects.md)

由 [desktop-media](../../packages/experimental/desktop-media/README.zh.md) Host 在 `$DSH_HOME/media/projects/` 保存带版本的 JSON、修订和镜头依赖。先落盘已验证的素材及不可变修订，再更新工程指针。同一工程写入串行化，使用预期修订核对提交，原子替换指针。同一修订的重复导出共享任务或已完成产物。项目指针与作品目录是两个文件。作品目录写入跨工程串行化，启动时按已提交导出记录幂等补索引。只有仍为当前修订的成功任务能推进默认成片。失败、取消和旧任务晚完成时保留上次成功成片与历史导出。

工程图不写入 SessionEventMap。确需新会话关联事件时，只保存可跳过的引用，并为新事件标记 `ignorable: true`。模型可见的工具结果须可从会话日志重建。工作台开关只停用项目编辑入口，既有媒体能力继续提供会话读取、作品目录及原生打开。验证关闭工作台后旧成片仍可定位和播放，并用缺少新事件类型的读取方回放。持久化变更按仓库流程审查。 [持久化类型变更](../cookbook/reviewing-persistence-type-changes.zh.md)

阶段 1 的必交预览走既有作品入口，由 Host 按作品 ID 解析并调用系统播放器。阶段 2 优先验证桌面内的认证 Host/Carrier 通道。保持 Tauri CSP，按工程所属工作区校验会话和作品访问。新会话恢复工程时校验同一工作区的访问权限，验证播放与拖动进度条。嵌入路径未通过时保留原生文件预览。需要交互式模板预览的功能，须先验证一条可用的交互路径再开放。若本地交互模板需要外部浏览器，阶段 0 另验证仅回环绑定、限时单工程授权、退出回收和文件访问限制。保留 Shotcraft 对素材导入的语义，固定端口开发服务与未经认证的 iframe 均不进入产品路径。 [原生文件打开](../../packages/experimental/desktop-media/src/local-open.ts) · [桌面 CSP](../../apps/desktop/src-tauri/tauri.conf.json)

## Shotcraft 与付费生成

保留真实截图、镜头卡到 demo 的映射、确定性运镜与静帧/成片检查。优先使用用户素材、经确认的真实截图及可程序绘制的元素。按常驻媒体工具的做法，工具定义保持精简并以 ID 返回。选择「镜头配方」后才读取所需卡片与模板，使用技能工具或限定文件读取。标准会话的系统提示词不包含 Shotcraft 目录。社交展示、剪映导出、自动字幕和卡点分析先留待后续。

Host 对自由创作、模板、回退及 worker 统一执行供应商白名单与付费准入。静态图用已验 MiniMax，视频用已验 MiniMax 或用户明确选择的 Seedance 2.0。桌面端可编辑方舟 Base URL、模型或接入点 ID，默认北京地址。Ark Key 与 MiniMax Key 独立，凭据由 Host 持有。发送前核对 HTTPS 目的地及重定向，凭据仅发往用户确认的可信方舟地址。确认绑定具体路线、素材、参数和费用上限，改路线或新增付费项须重新确认。模板不得调用 GPT 生图或绕过 Host。 [MiniMax 文生图](https://platform.minimax.cn/docs/api-reference/image-generation-t2i) · [Seedance 2.0](https://docs.volcengine.com/docs/ark/seedance-2-0?lang=zh)

先核对指纹和本地文件校验值，再列出新增生成项。复用镜头或旁白均为 0 次调用、¥0。使用现有会话预算默认值 8 次和 ¥40，现有媒体入口与工作台共享按会话原子核对及预留额度的逻辑，派发前核对当前修订及已确认的路线。已作废且尚未派发的预留可释放。费用按确认的模型、计费单位和数量估算，图像首版每请求 1 张。同一请求去重，重启仍能恢复占用。已提交或状态不明的费用继续占用额度，能查则查。无法确认原请求未计费时，禁止自动重发。跨会话重开工程仍须明确显示新会话额度和工程历史消费。 [现有媒体预算](../../packages/experimental/desktop-media/README.zh.md)

收到成果后立即下载到媒体库同目录的临时文件，验证大小、媒体格式和 SHA-256，原子移动到最终路径后才提交镜头版本。有供应商校验值时比对，没有时记录本地摘要并在复用时重验。测试中断、部分下载、过期和本地摘要不一致。链接失效时优先查询可恢复结果，保留既有版本。需要新生成时重新确认。

通过 [桌面技能清单](../../scripts/desktop-bundled-skills.json) 安装轻量技能和索引，通过 [桌面插件清单](../../scripts/desktop-profile-plugins.json) 安装工作台。固定 Shotcraft 提交、镜头卡、素材清单及校验值。已审核模板和专业组件按需安装，worker 不依赖开发机的全局技能、预装依赖或缓存。 [Shotcraft 源码](https://github.com/Vincentwei1021/video-shotcraft)

## 阶段 0 核验表

| 核验项 | 必须记录的证据与失败处理 |
| --- | --- |
| 安装与运行时 | 用 macOS arm64 干净账号，从 Finder 打开签名安装包。在无 Homebrew、系统 Node/Python、全局技能、额外浏览器和缓存条件下验证首次启动与明确的依赖提示。安装用户侧 FFmpeg 后用最小工程验证离线导入成片及取消与重启。用现有媒体能力验证升级、禁用与损坏恢复路径。记录来源、版本、校验值、安装包大小变化、下载与磁盘占用。 |
| 基础导出 | 用实际用户侧 FFmpeg 和 ffprobe 验证 `h264_videotoolbox`/AAC、横竖版、中文和空格路径。实跑 `subtitles`/`ass` 的 libass 烧录，用系统苹方或经许可的 Noto CJK 抽帧核对中文及标点。缺滤镜或字库时，验证可用的 MP4 与独立 SRT。对旁白与配乐验证基础增益混音。自动压低配乐或响度归一化若纳入首版，另实跑 `sidechaincompress`/`loudnorm` 并试听。 |
| 预览与专业渲染 | 从已安装桌面验证作品原生打开，再用最小专业预览验证工程授权与跨工程拒绝。对锁定的本地 Node、Remotion、Chrome/Chromium、字体和编码器验证下载来源、校验值、代理、失败恢复。用实际模板渲染静帧和成片，记录用时、占用、离线行为及预览和导出一致性。 |
| 许可与素材 | 分别记录 MoneyPrinterTurbo（MIT）、Shotcraft 代码（Apache-2.0）、demo、字体、音效及品牌图的许可、署名与实际分发范围。针对锁定的 Remotion 版本，形成书面结论，明确按需安装与由产品调用是否要求 Company License、谁持证、机构用户需接受什么条款。证据不足或条件未落实时专业版不放行。 |
| 国内 MiniMax | 用同一个国内按量 Key 通过 `api.minimax.cn` 验证 Image-01、`speech-2.8-hd` 和 MiniMax-H3-Max。公开文档基线为 Image-01 ¥0.025/张、URL 24 小时。用目标账户核对实际权限、尺寸和时长、成功计费张数、账单及链接下载。Image-01 未验通时仅开放已合格路线。 |
| Seedance 2.0 | 用中国大陆网络与具备开通权限的方舟账户测试可配置路线，记录目标模型 ID 或 endpoint ID、地区、额度、输入上传、提交/轮询、取消、重启恢复及实际费用。分别记录任务 ID 与下载链接有效期。只验证 2.0。 |
| 故障与计费 | 用经批准的测试额度与非敏感样本记录成功调用和账单。用可控样本验证限流、欠费、未知提交、并发超额、取消、过期、校验失败、磁盘不足和断网。记录失败提示、恢复动作、可保留的产物与费用。 |

阶段 0 的记录包含日期、构建/依赖版本、环境、可重放步骤、脱敏凭证与结论。阶段 1/2 通过 Host 行为测试、会话回放和安装后的正常入口。验收实际出片播放、听感、旧工程恢复、缺失素材、并发导出、旧任务迟到、跨会话修订与关闭工作台后的旧成片访问。 [Remotion 许可条款](https://www.remotion.dev/docs/license/) · [Shotcraft 许可](https://github.com/Vincentwei1021/video-shotcraft/blob/main/LICENSE) · [MoneyPrinterTurbo 许可](https://github.com/harry0703/MoneyPrinterTurbo/blob/86e45a2233a72c41d83389ac5cb49bbf5f6a847d/LICENSE) · [MiniMax 按量定价](https://platform.minimax.cn/docs/pricing/overview) · [方舟任务查询](https://docs.volcengine.com/docs/ark/get-video-generation-task-api?lang=zh)

## 现有桌面与微信入口

| 能力 | 当前入口及核实范围 |
| --- | --- |
| 控制这台 Mac | 新会话模式选择器提供「桌面操作」，设置「Agent 预设」可查看。源码与此前核对的 `0.1.2-rc.3` 包包含 `agent-desktop` 集成，实际点击和系统授权另行验证。 |
| 桌面 IM 接入 | 设置 → IM 接入支持飞书和钉钉机器人与身份绑定。 |
| 个人微信 | 桌面暂无已核实的专用配置页。飞书侧的个人微信操作经 Linux `server_ops`、broker 与已登录的微信 worker。公众号文章提取是独立技能。 |

[桌面操作预设](../../packages/experimental/desktop-operation/README.zh.md) · [预设界面](../../packages/client/ui-agent-preset/src/client/locales.ts) · [桌面 IM 渠道](../../packages/experimental/desktop-im/src/schema.ts) · [Linux 服务端配置](../../scripts/linux-operations/install.py)
