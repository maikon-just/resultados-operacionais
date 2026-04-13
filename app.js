const sidebar = document.getElementById('sidebar');
const mainContent = document.getElementById('mainContent');
const toggleSidebar = document.getElementById('toggleSidebar');
const navItems = document.querySelectorAll('.nav-item');
const contentFrame = document.getElementById('contentFrame');
const pageTitle = document.getElementById('pageTitle');
const pageDescription = document.getElementById('pageDescription');
const openCurrentPage = document.getElementById('openCurrentPage');

function syncLayout() {
  const isCollapsed = sidebar.classList.contains('collapsed');
  mainContent.classList.toggle('sidebar-collapsed', isCollapsed);
}

function setActivePage(button) {
  navItems.forEach(item => item.classList.remove('active'));
  button.classList.add('active');

  const page = button.dataset.page;
  const title = button.dataset.title;
  const description = button.dataset.description;

  contentFrame.src = page;
  contentFrame.title = title;
  pageTitle.textContent = title;
  pageDescription.textContent = description;
  openCurrentPage.href = page;
  document.title = title;
}

navItems.forEach(button => {
  button.addEventListener('click', () => setActivePage(button));
});

toggleSidebar.addEventListener('click', () => {
  sidebar.classList.toggle('collapsed');
  syncLayout();
});

syncLayout();
