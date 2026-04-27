import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { MessageCircle, Eye, EyeOff } from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { toast } from 'sonner';

const Login = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPwd, setShowPwd] = useState(false);
  const [loading, setLoading] = useState(false);
  const { login } = useAuth();
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!username || !password) {
      toast.error('Please fill in all fields');
      return;
    }
    setLoading(true);
    try {
      await login(username, password);
      toast.success('Welcome back');
      navigate('/app');
    } catch (err) {
      toast.error(err?.response?.data?.detail || 'Login failed');
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
            <h1 className="font-display text-3xl md:text-4xl font-extrabold tracking-tight">Welcome back</h1>
            <p className="mt-2 text-slate-600">Log in to continue automating your Instagram.</p>
            <form onSubmit={handleSubmit} className="mt-8 space-y-5">
              <div className="space-y-2">
                <Label htmlFor="username">Username</Label>
                <Input id="username" placeholder="yourname" value={username} onChange={e => setUsername(e.target.value)} className="h-12 rounded-xl" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="password">Password</Label>
                <div className="relative">
                  <Input id="password" type={showPwd ? 'text' : 'password'} placeholder="Password" value={password} onChange={e => setPassword(e.target.value)} className="h-12 rounded-xl pr-10" />
                  <button type="button" onClick={() => setShowPwd(!showPwd)} className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400">
                    {showPwd ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
                  </button>
                </div>
              </div>
              <Button type="submit" disabled={loading} className="w-full h-12 rounded-xl bg-slate-900 hover:bg-slate-800 text-white">
                {loading ? 'Signing in...' : 'Sign In'}
              </Button>
            </form>
            <p className="mt-6 text-sm text-center text-slate-600">
              Do not have an account? <Link to="/signup" className="font-semibold text-slate-900 hover:underline">Sign up</Link>
            </p>
            <p className="mt-4 text-xs text-center text-slate-500">
              <Link to="/privacy" className="hover:text-slate-900 underline">Privacy Policy</Link>
              <span className="mx-2">-</span>
              <Link to="/terms" className="hover:text-slate-900 underline">Terms of Service</Link>
            </p>
          </div>
        </div>
      </div>
      <div className="hidden md:block relative bg-gradient-to-br from-blue-500 via-purple-500 to-pink-500 overflow-hidden">
        <div className="absolute inset-0 flex items-center justify-center p-12">
          <div className="text-white max-w-md">
            <h2 className="font-display text-4xl font-extrabold leading-tight">Automate the conversations that grow your business.</h2>
            <p className="mt-4 text-white/90 text-lg">Use mychat with your own Instagram account, contacts, comments, and messages.</p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Login;
