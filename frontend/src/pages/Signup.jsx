import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { MessageCircle, Check } from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { toast } from 'sonner';

const Signup = () => {
  const [username, setUsername] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const { signup } = useAuth();
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!username || !email || !password) { toast.error('Please fill in all fields'); return; }
    if (password.length < 6) { toast.error('Password must be at least 6 characters'); return; }
    setLoading(true);
    try {
      await signup(username, email, password);
      toast.success('Account created! Welcome to mychat');
      navigate('/app');
    } catch (err) {
      toast.error(err?.response?.data?.detail || 'Signup failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen grid md:grid-cols-2 bg-white">
      <div className="flex flex-col p-8 md:p-12">
        <Link to="/" className="flex items-center gap-2">
          <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
            <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
          </div>
          <span className="text-xl font-bold font-display">mychat</span>
        </Link>
        <div className="flex-1 flex items-center justify-center">
          <div className="w-full max-w-sm">
            <h1 className="font-display text-3xl md:text-4xl font-extrabold tracking-tight">Create your account</h1>
            <p className="mt-2 text-slate-600">Free forever. No credit card required.</p>
            <form onSubmit={handleSubmit} className="mt-8 space-y-4">
              <div className="space-y-2">
                <Label htmlFor="username">Username</Label>
                <Input id="username" placeholder="yourname" value={username} onChange={e => setUsername(e.target.value)} className="h-12 rounded-xl" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="email">Email</Label>
                <Input id="email" type="email" placeholder="you@company.com" value={email} onChange={e => setEmail(e.target.value)} className="h-12 rounded-xl" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="password">Password</Label>
                <Input id="password" type="password" placeholder="At least 6 characters" value={password} onChange={e => setPassword(e.target.value)} className="h-12 rounded-xl" />
              </div>
              <Button type="submit" disabled={loading} className="w-full h-12 rounded-xl bg-slate-900 hover:bg-slate-800 text-white">
                {loading ? 'Creating account...' : 'Create Account'}
              </Button>
            </form>
            <p className="mt-6 text-sm text-center text-slate-600">
              Already have one? <Link to="/login" className="font-semibold text-slate-900 hover:underline">Log in</Link>
            </p>
          </div>
        </div>
      </div>
      <div className="hidden md:flex relative bg-gradient-to-br from-pink-500 via-orange-400 to-amber-400 overflow-hidden items-center justify-center p-12">
        <div className="text-white max-w-md">
          <h2 className="font-display text-4xl font-extrabold leading-tight">Your Instagram assistant that never sleeps.</h2>
          <ul className="mt-8 space-y-4">
            {['Reply to comments in seconds', 'Convert DMs into sales', 'Build audience segments automatically', 'Track every conversion'].map(t => (
              <li key={t} className="flex items-center gap-3 bg-white/10 backdrop-blur-sm rounded-xl px-4 py-3 border border-white/20">
                <div className="w-6 h-6 rounded-full bg-white/20 flex items-center justify-center"><Check className="w-4 h-4" /></div>
                <span className="font-medium">{t}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
};

export default Signup;
