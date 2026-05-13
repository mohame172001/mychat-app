# Accessibility Checklist

- Target: WCAG 2.2 AA concepts for critical login, signup, legal, dashboard, automations, comments, billing placeholder, system health, and admin flows.
- Current evidence: frontend unit tests and Playwright smoke verify login/signup/legal/product/admin rendering and key controls.
- Before payment launch: manually keyboard-test login, signup, Google Sign-In, billing placeholder, data deletion, and admin destructive confirmations.
- Do not add CAPTCHA-only auth without accessible alternative.
- Treat full external WCAG audit as P3 unless login/signup/payment becomes blocked.
