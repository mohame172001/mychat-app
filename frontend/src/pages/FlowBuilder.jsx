import React, { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Badge } from '../components/ui/badge';
import {
  ArrowLeft, Save, Play, MessageCircle, Zap, GitBranch, Tag, Clock,
  Send, Image, Hash, Plus, MoreHorizontal
} from 'lucide-react';
import { flowNodes as initialNodes, flowEdges } from '../mock/mock';
import api from '../lib/api';
import { toast } from 'sonner';

const blockTypes = [
  { type: 'trigger', label: 'Trigger', icon: Zap, color: 'from-pink-500 to-orange-400' },
  { type: 'message', label: 'Send Message', icon: MessageCircle, color: 'from-blue-500 to-cyan-400' },
  { type: 'condition', label: 'Condition', icon: GitBranch, color: 'from-amber-500 to-yellow-400' },
  { type: 'action', label: 'Add Tag', icon: Tag, color: 'from-emerald-500 to-teal-400' },
  { type: 'delay', label: 'Delay', icon: Clock, color: 'from-purple-500 to-pink-400' },
  { type: 'media', label: 'Send Image', icon: Image, color: 'from-indigo-500 to-blue-400' }
];

const FlowBuilder = () => {
  const { id } = useParams();
  const [nodes, setNodes] = useState(initialNodes);
  const [name, setName] = useState('Untitled Automation');
  const [status, setStatus] = useState('draft');
  const [selectedId, setSelectedId] = useState(null);

  useEffect(() => {
    if (!id || id === 'new') return;
    (async () => {
      try {
        const { data } = await api.get(`/automations/${id}`);
        setName(data.name);
        setStatus(data.status);
        if (data.nodes && data.nodes.length > 0) setNodes(data.nodes);
      } catch (err) {
        console.error('[FlowBuilder] Failed to load automation:', err);
      }
    })();
  }, [id]);

  const handleSave = async () => {
    try {
      if (id && id !== 'new') {
        await api.patch(`/automations/${id}`, { name, nodes });
      }
      toast.success('Flow saved');
    } catch { toast.error('Save failed'); }
  };

  const handlePublish = async () => {
    try {
      if (id && id !== 'new') {
        await api.patch(`/automations/${id}`, { name, nodes, status: 'active' });
        setStatus('active');
      }
      toast.success('Flow published!');
    } catch { toast.error('Publish failed'); }
  };

  const addNode = (type) => {
    const block = blockTypes.find(b => b.type === type);
    const newNode = {
      id: `n${Date.now()}`,
      type,
      title: block.label,
      subtitle: 'Click to configure',
      x: 100 + nodes.length * 40,
      y: 260 + nodes.length * 20,
      color: block.color
    };
    setNodes([...nodes, newNode]);
    toast.success(`${block.label} added to flow`);
  };

  const selected = nodes.find(n => n.id === selectedId);

  const getEdgePath = (from, to) => {
    const fx = from.x + 224, fy = from.y + 40;
    const tx = to.x, ty = to.y + 40;
    const midX = (fx + tx) / 2;
    return `M ${fx} ${fy} C ${midX} ${fy}, ${midX} ${ty}, ${tx} ${ty}`;
  };

  return (
    <div className="h-[calc(100vh-4rem)] flex flex-col bg-slate-50">
      {/* Toolbar */}
      <div className="h-14 bg-white border-b border-slate-200 px-6 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          <Link to="/app/automations"><Button variant="ghost" size="icon" className="rounded-lg"><ArrowLeft className="w-4 h-4" /></Button></Link>
          <Input value={name} onChange={e => setName(e.target.value)} className="h-9 w-72 border-transparent hover:border-slate-200 rounded-lg font-semibold" />
          <Badge className={`rounded-full ${status === 'active' ? 'bg-emerald-50 text-emerald-700 border-emerald-100' : 'bg-amber-50 text-amber-700 border-amber-100'}`}>{status}</Badge>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" className="rounded-xl" onClick={handleSave}><Save className="w-4 h-4 mr-1.5" /> Save</Button>
          <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl" onClick={handlePublish}><Play className="w-4 h-4 mr-1.5" /> Publish</Button>
        </div>
      </div>

      <div className="flex-1 flex overflow-hidden">
        {/* Blocks palette */}
        <aside className="w-64 bg-white border-r border-slate-200 p-4 overflow-y-auto">
          <h3 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3">Blocks</h3>
          <div className="space-y-2">
            {blockTypes.map(b => {
              const Icon = b.icon;
              return (
                <button key={b.type} onClick={() => addNode(b.type)} className="w-full flex items-center gap-3 p-3 rounded-xl border border-slate-100 hover:border-slate-300 hover:shadow-sm transition-all text-left group">
                  <div className={`w-9 h-9 rounded-lg bg-gradient-to-br ${b.color} flex items-center justify-center shrink-0`}>
                    <Icon className="w-4 h-4 text-white" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-semibold">{b.label}</div>
                    <div className="text-xs text-slate-500">Click to add</div>
                  </div>
                  <Plus className="w-4 h-4 text-slate-400 opacity-0 group-hover:opacity-100" />
                </button>
              );
            })}
          </div>
        </aside>

        {/* Canvas */}
        <div className="flex-1 relative overflow-auto flow-grid">
          <svg className="absolute inset-0 w-full h-full pointer-events-none" style={{ minWidth: 1400, minHeight: 600 }}>
            {flowEdges.map((e) => {
              const fromNode = nodes.find(n => n.id === e.from);
              const toNode = nodes.find(n => n.id === e.to);
              if (!fromNode || !toNode) return null;
              return (
                <path key={`${e.from}-${e.to}`} d={getEdgePath(fromNode, toNode)} stroke="#94a3b8" strokeWidth="2" fill="none" strokeDasharray="6 4" />
              );
            })}
          </svg>
          <div className="relative" style={{ minWidth: 1400, minHeight: 600 }}>
            {nodes.map(n => {
              const block = blockTypes.find(b => b.type === n.type);
              const Icon = block?.icon || Hash;
              return (
                <div
                  key={n.id}
                  onClick={() => setSelectedId(n.id)}
                  className={`absolute w-56 rounded-2xl shadow-lg cursor-pointer transition-all hover:-translate-y-0.5 hover:shadow-xl ${selectedId === n.id ? 'ring-2 ring-offset-2 ring-slate-900' : ''}`}
                  style={{ left: n.x, top: n.y }}
                >
                  <div className={`rounded-t-2xl bg-gradient-to-br ${n.color} p-3 flex items-center gap-2`}>
                    <Icon className="w-4 h-4 text-white" />
                    <span className="text-xs font-semibold text-white uppercase tracking-wider">{n.type}</span>
                    <MoreHorizontal className="w-4 h-4 text-white/70 ml-auto" />
                  </div>
                  <div className="bg-white rounded-b-2xl p-4">
                    <div className="font-semibold text-sm">{n.title}</div>
                    <div className="text-xs text-slate-500 mt-1 line-clamp-2">{n.subtitle}</div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Inspector */}
        <aside className="w-80 bg-white border-l border-slate-200 p-5 overflow-y-auto">
          {selected ? (
            <div>
              <div className="flex items-center gap-2 mb-2">
                <Badge className="bg-slate-100 text-slate-700 border-0 rounded-full capitalize">{selected.type}</Badge>
              </div>
              <h3 className="font-display font-bold text-lg">{selected.title}</h3>
              <div className="mt-5 space-y-4">
                <div>
                  <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Title</label>
                  <Input value={selected.title} readOnly className="mt-1.5 rounded-xl" />
                </div>
                <div>
                  <label className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Message</label>
                  <textarea defaultValue={selected.subtitle} className="mt-1.5 w-full min-h-[120px] px-3 py-2 rounded-xl border border-slate-200 text-sm focus:outline-none focus:ring-2 focus:ring-slate-900" />
                </div>
                <Button className="w-full bg-slate-900 hover:bg-slate-800 text-white rounded-xl"><Send className="w-4 h-4 mr-1.5" /> Test Block</Button>
              </div>
            </div>
          ) : (
            <div className="text-center mt-10">
              <div className="w-14 h-14 mx-auto rounded-2xl bg-slate-100 flex items-center justify-center"><Hash className="w-6 h-6 text-slate-400" /></div>
              <h3 className="mt-4 font-display font-bold">Select a block</h3>
              <p className="text-sm text-slate-500 mt-1">Click any block on the canvas to configure its settings.</p>
            </div>
          )}
        </aside>
      </div>
    </div>
  );
};

export default FlowBuilder;
