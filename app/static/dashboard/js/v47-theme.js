// v47-theme.js — normalize to light mode
document.addEventListener('DOMContentLoaded', function(){
  document.documentElement.classList.remove('dark');
  document.body.classList.remove('dark');
  document.documentElement.removeAttribute('data-theme');
  document.body.removeAttribute('data-theme');
});