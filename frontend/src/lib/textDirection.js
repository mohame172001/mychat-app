const RTL_RE = /[\u0590-\u05FF\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]/;

export const detectTextDirection = (text = '') => {
  const value = String(text || '');
  const isRtl = RTL_RE.test(value);
  return {
    dir: isRtl ? 'rtl' : 'ltr',
    align: isRtl ? 'right' : 'left',
  };
};

export const autoDirStyle = (text = {}) => {
  const { dir, align } = detectTextDirection(text);
  return {
    direction: dir,
    textAlign: align,
    unicodeBidi: 'plaintext',
    overflowWrap: 'anywhere',
    whiteSpace: 'pre-wrap',
  };
};
