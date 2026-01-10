document.querySelectorAll('th[data-sort]').forEach((header) => {
  header.addEventListener('click', () => {
    const table = header.closest('table');
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    const index = Array.from(header.parentNode.children).indexOf(header);
    const direction = header.dataset.direction === 'asc' ? 'desc' : 'asc';
    header.dataset.direction = direction;
    rows.sort((a, b) => {
      const aText = a.children[index].dataset.value || a.children[index].innerText;
      const bText = b.children[index].dataset.value || b.children[index].innerText;
      const aNum = parseFloat(aText);
      const bNum = parseFloat(bText);
      if (!Number.isNaN(aNum) && !Number.isNaN(bNum)) {
        return direction === 'asc' ? aNum - bNum : bNum - aNum;
      }
      return direction === 'asc' ? aText.localeCompare(bText) : bText.localeCompare(aText);
    });
    rows.forEach((row) => tbody.appendChild(row));
  });
});