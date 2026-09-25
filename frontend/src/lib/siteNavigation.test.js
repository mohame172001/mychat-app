import fs from 'fs';
import path from 'path';
import { parse } from '@babel/parser';
import { ROUTES } from '../constants/routes';

const files = [
  'pages/Landing.jsx', 'pages/Login.jsx', 'pages/Signup.jsx',
  'pages/ForgotPassword.jsx', 'pages/ResetPassword.jsx', 'pages/VerifyEmail.jsx',
  'pages/Support.jsx', 'pages/PrivacyPolicy.jsx', 'pages/Terms.jsx',
  'pages/DataDeletion.jsx', 'pages/NotFound.jsx', 'pages/StatusPage.jsx',
  'pages/Dashboard.jsx', 'pages/Automations.jsx', 'pages/FlowBuilder.jsx',
  'pages/Settings.jsx', 'pages/Billing.jsx', 'pages/DmAutomation.jsx',
  'components/layout/Sidebar.jsx', 'components/layout/Topbar.jsx',
  'components/layout/DashboardLayout.jsx',
];
const known = new Set([...Object.values(ROUTES).map(p => p.split('?')[0]), '/contact']);
function walk(node, visit) {
  if (!node || typeof node !== 'object') return;
  if (node.type) visit(node);
  for (const value of Object.values(node)) {
    if (Array.isArray(value)) value.forEach(child => walk(child, visit));
    else if (value && typeof value === 'object') walk(value, visit);
  }
}

test.each(files)('%s points static navigation at real routes', file => {
  const source = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
  const ast = parse(source, { sourceType: 'module', plugins: ['jsx'] });
  const destinations = [];
  walk(ast, node => {
    if (node.type === 'JSXAttribute' && ['href', 'to'].includes(node.name?.name)
        && node.value?.type === 'StringLiteral') destinations.push(node.value.value);
    if (node.type === 'CallExpression' && node.callee?.name === 'navigate'
        && node.arguments[0]?.type === 'StringLiteral') destinations.push(node.arguments[0].value);
  });
  for (const destination of destinations.filter(p => p.startsWith('/') && !p.startsWith('//'))) {
    const pathname = destination.split(/[?#]/)[0];
    expect({ destination, registered: known.has(pathname) || /^\/app\/automations\/[\w-]+$/.test(pathname) })
      .toEqual({ destination, registered: true });
  }
});

test('landing section links have matching targets', () => {
  const source = fs.readFileSync(path.join(__dirname, '../pages/Landing.jsx'), 'utf8');
  for (const [, anchor] of source.matchAll(/href="#([\w-]+)"/g)) {
    expect(source).toContain(`id="${anchor}"`);
  }
});

test('public support links do not hardcode email addresses outside the shared helper', () => {
  for (const file of files) {
    const source = fs.readFileSync(path.join(__dirname, '..', file), 'utf8');
    expect(source).not.toMatch(/mailto:[^"`]*gmail\.com/);
  }
});
