# Flow visual system

- Landing: scroll-linked post, comment, rule, and message illustration. Four keyboard-accessible stage controls; reduced-motion users can switch stages without a long scrolling sequence. Examples are explicitly illustrative and never call Instagram.
- Arabic: existing self-hosted Alexandria, bold headings, natural Arabic spacing. No remote font dependency added.
- Shared UI: petrol text and primary actions, mint surfaces, restrained lime accents, rounded buttons and cards. Status/error colors preserved.
- Workspace: translucent header, calm background, existing navigation/auth/admin gating unchanged.
- Login/signup: matching visual identity; existing form handlers and redirects unchanged.
- Public/legal/support pages inherit shared typography and palette; legal copy and support delivery unchanged.
- No backend, schema, billing, tokens, or account changes.

Validation: frontend Jest suite and production build; browser checks of public routes and responsive landing. Authenticated backend behavior requires a live session and is not simulated by the local static preview.
