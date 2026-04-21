// Mock data for mychat - ManyChat-like Instagram automation platform

export const features = [
  {
    icon: 'MessageCircle',
    title: 'Instagram DM Automation',
    description: 'Automatically respond to comments, story replies and DMs with personalized messages that convert.',
    color: 'from-pink-500 to-orange-400'
  },
  {
    icon: 'Zap',
    title: 'Instant Reactions',
    description: 'Trigger automations the moment someone comments a keyword on your posts or reels.',
    color: 'from-blue-500 to-cyan-400'
  },
  {
    icon: 'Users',
    title: 'Grow Your Audience',
    description: 'Turn every interaction into a new follower, lead, or customer with smart flows.',
    color: 'from-purple-500 to-pink-400'
  },
  {
    icon: 'BarChart3',
    title: 'Analytics That Matter',
    description: 'Track engagement, conversions, and revenue from every automated conversation.',
    color: 'from-emerald-500 to-teal-400'
  },
  {
    icon: 'Bot',
    title: 'AI-Powered Replies',
    description: 'Let AI handle FAQs while you focus on high-value conversations.',
    color: 'from-indigo-500 to-blue-400'
  },
  {
    icon: 'Target',
    title: 'Smart Targeting',
    description: 'Segment contacts by behavior, tags and custom fields for perfect personalization.',
    color: 'from-amber-500 to-orange-400'
  }
];

export const stats = [
  { value: '1M+', label: 'Businesses Automated' },
  { value: '4B+', label: 'Conversations Sent' },
  { value: '190+', label: 'Countries Worldwide' },
  { value: '80%', label: 'Higher Engagement' }
];

export const testimonials = [
  {
    name: 'Sarah Johnson',
    role: 'Founder, BeautyBox',
    avatar: 'https://i.pravatar.cc/150?img=47',
    quote: 'mychat tripled our Instagram sales in 30 days. The comment-to-DM flow is pure magic.',
    rating: 5
  },
  {
    name: 'Ahmed Hassan',
    role: 'Fitness Coach',
    avatar: 'https://i.pravatar.cc/150?img=12',
    quote: 'I went from manually replying to 500 DMs a day to fully automated. Got my life back.',
    rating: 5
  },
  {
    name: 'Emma Chen',
    role: 'E-commerce Owner',
    avatar: 'https://i.pravatar.cc/150?img=45',
    quote: 'The ROI is insane. Every post now converts comments into customers automatically.',
    rating: 5
  }
];

export const pricingPlans = [
  {
    name: 'Free',
    price: 0,
    description: 'Perfect to get started',
    features: ['Up to 1,000 contacts', 'Basic automations', 'Comment auto-reply', 'Community support'],
    cta: 'Get Started',
    popular: false
  },
  {
    name: 'Pro',
    price: 15,
    description: 'For growing businesses',
    features: ['Unlimited contacts', 'Advanced flow builder', 'AI replies', 'Analytics dashboard', 'Priority support', 'Custom fields & tags'],
    cta: 'Start Free Trial',
    popular: true
  },
  {
    name: 'Business',
    price: 45,
    description: 'For teams at scale',
    features: ['Everything in Pro', 'Team collaboration', 'Advanced integrations', 'API access', 'Dedicated manager', 'Custom training'],
    cta: 'Contact Sales',
    popular: false
  }
];

export const dashboardStats = [
  { label: 'Total Contacts', value: '12,847', change: '+18.2%', trend: 'up', icon: 'Users' },
  { label: 'Active Automations', value: '24', change: '+3', trend: 'up', icon: 'Zap' },
  { label: 'Messages Sent', value: '148,392', change: '+24.5%', trend: 'up', icon: 'Send' },
  { label: 'Conversion Rate', value: '32.4%', change: '+2.1%', trend: 'up', icon: 'TrendingUp' }
];

export const chartData = [
  { day: 'Mon', messages: 3200, conversions: 980 },
  { day: 'Tue', messages: 4100, conversions: 1240 },
  { day: 'Wed', messages: 3800, conversions: 1180 },
  { day: 'Thu', messages: 5200, conversions: 1680 },
  { day: 'Fri', messages: 6100, conversions: 2020 },
  { day: 'Sat', messages: 4800, conversions: 1520 },
  { day: 'Sun', messages: 4300, conversions: 1380 }
];

export const automations = [
  { id: '1', name: 'Welcome New Followers', trigger: 'New Follower', status: 'active', sent: 2847, clicks: 892, updated: '2 hours ago' },
  { id: '2', name: 'Comment to DM - Product Launch', trigger: 'Keyword: LAUNCH', status: 'active', sent: 5621, clicks: 1834, updated: '5 hours ago' },
  { id: '3', name: 'Story Reply Auto-Response', trigger: 'Story Reply', status: 'active', sent: 1203, clicks: 421, updated: '1 day ago' },
  { id: '4', name: 'Abandoned Cart Recovery', trigger: 'Custom Event', status: 'paused', sent: 847, clicks: 302, updated: '3 days ago' },
  { id: '5', name: 'VIP Customer Flow', trigger: 'Tag Added', status: 'active', sent: 412, clicks: 198, updated: '1 week ago' },
  { id: '6', name: 'Lead Magnet Giveaway', trigger: 'Keyword: FREEBIE', status: 'draft', sent: 0, clicks: 0, updated: '2 weeks ago' }
];

export const contacts = [
  { id: '1', name: 'Jessica Martinez', username: '@jessicam', avatar: 'https://i.pravatar.cc/150?img=1', tags: ['Customer', 'VIP'], lastActive: '2 min ago', subscribed: true },
  { id: '2', name: 'Michael Brown', username: '@mikebrown', avatar: 'https://i.pravatar.cc/150?img=3', tags: ['Lead'], lastActive: '15 min ago', subscribed: true },
  { id: '3', name: 'Olivia Wilson', username: '@oliviaw', avatar: 'https://i.pravatar.cc/150?img=5', tags: ['Customer'], lastActive: '1 hour ago', subscribed: true },
  { id: '4', name: 'Daniel Garcia', username: '@dangarcia', avatar: 'https://i.pravatar.cc/150?img=7', tags: ['Prospect'], lastActive: '3 hours ago', subscribed: false },
  { id: '5', name: 'Sophia Rodriguez', username: '@sophiar', avatar: 'https://i.pravatar.cc/150?img=9', tags: ['Customer', 'VIP'], lastActive: '5 hours ago', subscribed: true },
  { id: '6', name: 'James Anderson', username: '@jamesa', avatar: 'https://i.pravatar.cc/150?img=11', tags: ['Lead'], lastActive: '1 day ago', subscribed: true },
  { id: '7', name: 'Isabella Thomas', username: '@bellat', avatar: 'https://i.pravatar.cc/150?img=14', tags: ['Customer'], lastActive: '2 days ago', subscribed: true },
  { id: '8', name: 'William Martinez', username: '@willm', avatar: 'https://i.pravatar.cc/150?img=15', tags: ['Unsubscribed'], lastActive: '1 week ago', subscribed: false }
];

export const conversations = [
  {
    id: '1',
    contact: { name: 'Jessica Martinez', username: '@jessicam', avatar: 'https://i.pravatar.cc/150?img=1' },
    lastMessage: 'Is this still in stock?',
    time: '2m',
    unread: 2,
    messages: [
      { id: 'm1', from: 'contact', text: 'Hey! Saw your latest post', time: '10:42 AM' },
      { id: 'm2', from: 'contact', text: 'Is this still in stock?', time: '10:43 AM' },
      { id: 'm3', from: 'me', text: 'Hi Jessica! Yes it is available in all sizes.', time: '10:45 AM' }
    ]
  },
  {
    id: '2',
    contact: { name: 'Michael Brown', username: '@mikebrown', avatar: 'https://i.pravatar.cc/150?img=3' },
    lastMessage: 'Thanks, got the discount code!',
    time: '15m',
    unread: 0,
    messages: [
      { id: 'm1', from: 'contact', text: 'LAUNCH', time: '9:30 AM' },
      { id: 'm2', from: 'me', text: 'Welcome! Here\u2019s your 20% off code: LAUNCH20', time: '9:30 AM' },
      { id: 'm3', from: 'contact', text: 'Thanks, got the discount code!', time: '9:31 AM' }
    ]
  },
  {
    id: '3',
    contact: { name: 'Olivia Wilson', username: '@oliviaw', avatar: 'https://i.pravatar.cc/150?img=5' },
    lastMessage: 'Perfect, when does it arrive?',
    time: '1h',
    unread: 1,
    messages: [
      { id: 'm1', from: 'contact', text: 'Perfect, when does it arrive?', time: '9:15 AM' }
    ]
  },
  {
    id: '4',
    contact: { name: 'Daniel Garcia', username: '@dangarcia', avatar: 'https://i.pravatar.cc/150?img=7' },
    lastMessage: 'Can I get a refund?',
    time: '3h',
    unread: 0,
    messages: [
      { id: 'm1', from: 'contact', text: 'Can I get a refund?', time: '7:20 AM' }
    ]
  }
];

export const broadcasts = [
  { id: '1', name: 'Black Friday Sale Announcement', status: 'sent', audience: 8421, openRate: '68%', clickRate: '32%', date: 'Nov 24, 2025' },
  { id: '2', name: 'New Product Launch', status: 'scheduled', audience: 12847, openRate: '-', clickRate: '-', date: 'Dec 1, 2025' },
  { id: '3', name: 'Weekly Newsletter #47', status: 'sent', audience: 7832, openRate: '54%', clickRate: '21%', date: 'Nov 18, 2025' },
  { id: '4', name: 'VIP Early Access', status: 'draft', audience: 1240, openRate: '-', clickRate: '-', date: '-' }
];

export const flowNodes = [
  { id: 'n1', type: 'trigger', title: 'Comment Trigger', subtitle: 'Keyword: LAUNCH', x: 80, y: 80, color: 'from-pink-500 to-orange-400' },
  { id: 'n2', type: 'message', title: 'Send Message', subtitle: 'Welcome! Thanks for your interest...', x: 380, y: 80, color: 'from-blue-500 to-cyan-400' },
  { id: 'n3', type: 'condition', title: 'Condition', subtitle: 'If tag = Customer', x: 680, y: 80, color: 'from-amber-500 to-yellow-400' },
  { id: 'n4', type: 'message', title: 'VIP Offer', subtitle: 'Exclusive 30% off just for you', x: 980, y: 20, color: 'from-purple-500 to-pink-400' },
  { id: 'n5', type: 'action', title: 'Add Tag', subtitle: 'Tag: Interested', x: 980, y: 160, color: 'from-emerald-500 to-teal-400' }
];

export const flowEdges = [
  { from: 'n1', to: 'n2' },
  { from: 'n2', to: 'n3' },
  { from: 'n3', to: 'n4' },
  { from: 'n3', to: 'n5' }
];
