for (const [platform, installer] of Object.entries(installers)) {
  const size = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 1 }).format(installer.bytes / 1e6);
  document.querySelector(`[data-release="${platform}"]`).textContent = `${installer.version} · ${installer.commit} · ${size} MB`;
  document.querySelector(`[data-checksum="${platform}"]`).textContent = `SHA-256 ${installer.sha256}`;

  for (const link of document.querySelectorAll(`[data-installer="${platform}"]`)) {
    if (installer.url) {
      link.href = installer.url;
      link.download = installer.file;
    } else if (link.closest('.installer')) {
      link.hidden = true;
      const status = document.querySelector(`[data-status="${platform}"]`);
      status.textContent = '下载链接准备中。';
      status.hidden = false;
    }
  }
}

const scenes = {
  knowledge: {
    title: '问知识',
    description: '把相关文档放进知识库，等待处理完成后提问，再打开引用核对原文。',
    image: './assets/knowledge.webp',
    alt: '小绿鲸知识库界面，显示文档名称和处理状态',
  },
  data: {
    title: '问数据',
    description: '上传 Excel 或 CSV 后，先确认数据表，再提问。示例画面展示工作区中返回的表格分析。',
    image: './assets/data.webp',
    alt: '小绿鲸数据工作区中的表格分析结果',
  },
  team: {
    title: '团队协作',
    description: '在活动策划任务里，队友分别收集资料、搭提纲、写文案和审稿，主控汇总可交付的结果。需要第二意见时，macOS 构建可委派给本机已登录的 Codex 或 CodeBuddy CLI。',
    image: './assets/team-output.webp',
    alt: '文案团队交付的活动传播文案示例',
  },
};

const dialog = document.querySelector('#scene');
for (const button of document.querySelectorAll('[data-scene]')) {
  button.addEventListener('click', () => {
    const scene = scenes[button.dataset.scene];
    document.querySelector('#scene-title').textContent = scene.title;
    document.querySelector('#scene-description').textContent = scene.description;
    const image = document.querySelector('#scene-image');
    image.alt = scene.alt;
    image.src = scene.image;
    dialog.showModal();
  });
}
