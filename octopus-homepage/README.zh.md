# Octopus 首页

[English](README.md) | 中文

在仓库根目录运行：

```sh
python3 octopus-homepage/preview.py
```

打开 <http://127.0.0.1:4173/>。预览服务核对 `dist` 中两个安装包和发布清单的大小与 SHA-256，再提供本机下载链接。需要更换端口时传入 `--port`。

部署时将 `site/` 作为 EdgeOne Pages 的静态站点根目录。将安装包上传到 COS，再填写 `site/downloads.js` 中各自的 HTTPS 下载地址。更新安装包时，从对应的发布清单同步版本、提交号、大小和 SHA-256。未配置链接时，站点会显示准备中。

首屏使用仓库里的 Team Room 实录，场景图库使用已有的产品截图和团队产出。

## 发布新安装包

在发布工作站上做一次设置。安装腾讯云 COS SDK，并让仓库的提交钩子使用这个 Python 环境。

```sh
python3 -m venv .git/octopus-cos
.git/octopus-cos/bin/python -m pip install 'cos-python-sdk-v5==1.9.45'
git config --local octopus.cosPython "$(git rev-parse --show-toplevel)/.git/octopus-cos/bin/python"
```

在创建发布提交的终端设置 `COS_SECRET_ID` 和 `COS_SECRET_KEY`。使用 CAM 临时凭据时，还要设置 `COS_SESSION_TOKEN`。使用仅有 `octopusapp-1322755166` 桶内 `releases/` 路径上传及设置对象 ACL 权限的 CAM 身份。钩子在 `master` 上通过仓库现有的 Lefthook 运行。 macOS 上可以在「钥匙串访问」里创建通用密码项 `octopus-cos-publisher`，账户名填 SecretId，密码填 SecretKey。然后运行一次 `git config --local octopus.cosSecretId YOUR_SECRET_ID`。

构建并验证需要发布的平台，把标准命名的安装包和发布清单放在 `dist/`。暂不升级的平台可以沿用现有安装包。新清单的源码提交必须已进入当前分支，并且晚于该平台已发布的源码提交。提交时，钩子会依据清单校验文件，上传新版到固定的版本路径，验证匿名访问，再把 `site/downloads.js` 加入同一个提交。将该提交推送到 EdgeOne 绑定的仓库和分支。没有符合条件的新版安装包就不会上传。只发布安装包时，可以直接运行 `git commit -m "release: update installer downloads"` 再推送。
