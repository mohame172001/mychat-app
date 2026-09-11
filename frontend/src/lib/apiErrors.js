function detailToMessage(detail) {
  if (!detail) return '';
  if (typeof detail === 'string') return detail;
  if (Array.isArray(detail)) {
    return detail.map(detailToMessage).filter(Boolean).join(' ');
  }
  if (typeof detail === 'object') {
    return detailToMessage(
      detail.message || detail.detail || detail.error || detail.code,
    );
  }
  return String(detail);
}

export function apiErrorMessage(error, fallback) {
  const data = error?.response?.data;
  return detailToMessage(data?.detail || data?.message || data?.error || error?.message)
    || fallback;
}
