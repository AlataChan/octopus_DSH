# Octopus 首页

[English](README.md) | 中文

在仓库根目录运行：

```sh
python3 octopus-homepage/preview.py
```

打开 <http://127.0.0.1:4173/>。预览服务核对 `dist` 中两个安装包和发布清单的大小与 SHA-256，再提供本机下载链接。需要更换端口时传入 `--port`。

部署时将 `site/` 作为 EdgeOne Pages 的静态站点根目录。将安装包上传到 COS，再填写 `site/downloads.js` 中各自的 HTTPS 下载地址。更新安装包时，从对应的发布清单同步版本、提交号、大小和 SHA-256。未配置链接时，站点会显示准备中。

首屏使用仓库里的 Team Room 实录，场景图库使用已有的产品截图和团队产出。
