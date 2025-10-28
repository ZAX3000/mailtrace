(() => {
  const panel = getComputedStyle(document.body).getPropertyValue('--dash-panel').trim() || '#E6F1FF';
  document.documentElement.style.setProperty('--mapper-bg', panel);
})();