/**
 * Phase 2.18Z — single source of truth for the support contact action.
 *
 * The Sidebar 'Help & Support' button and the footer 'Contact' link
 * used to be plain `mailto:` anchors. On any device without a default
 * mail client configured (which is most modern browsers by default),
 * clicking did nothing — the click silently no-op'd with zero visible
 * feedback, so users assumed the button was broken.
 *
 * `handleContactClick` keeps the mailto attempt for users who DO have
 * a mail client, but ALSO copies the support email to the clipboard
 * and shows a clear toast so the action is never silent. The email
 * itself is centralised here so changing it touches one place.
 */
import { toast } from 'sonner';

export const SUPPORT_EMAIL = 'mm.mohame172000@gmail.com';
export const SUPPORT_MAILTO_SUBJECT = 'MyChat support request';

export function buildSupportMailtoHref() {
  const subject = encodeURIComponent(SUPPORT_MAILTO_SUBJECT);
  return `mailto:${SUPPORT_EMAIL}?subject=${subject}`;
}

async function copyToClipboard(text) {
  if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (_) {
      // fall through to legacy path below
    }
  }
  // Legacy fallback for older browsers / sandboxed iframes.
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.setAttribute('readonly', '');
    ta.style.position = 'absolute';
    ta.style.left = '-9999px';
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand('copy');
    document.body.removeChild(ta);
    return ok;
  } catch (_) {
    return false;
  }
}

/**
 * Click handler for any "Help & Support" / "Contact" button. Returns
 * the click event so consumers can spread it onto an <a> tag (we don't
 * preventDefault — the browser still tries the mailto: link in
 * parallel so users who DO have a mail client get the friendly path).
 */
export async function handleContactClick(/* event */) {
  const copied = await copyToClipboard(SUPPORT_EMAIL);
  if (copied) {
    toast.success(`Support email copied: ${SUPPORT_EMAIL}`, {
      description: 'Paste it into your mail client to reach us.',
      duration: 6000,
    });
  } else {
    toast.message(`Contact: ${SUPPORT_EMAIL}`, {
      description: "We couldn't auto-copy — please copy this address manually.",
      duration: 8000,
    });
  }
}
