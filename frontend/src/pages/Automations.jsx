import React, { useEffect, useMemo, useState } from 'react';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { Input } from '../components/ui/input';
import { Switch } from '../components/ui/switch';
import { Checkbox } from '../components/ui/checkbox';
import {
  ArrowLeft, Bookmark, CheckCircle2, Circle, Filter, Hash, Heart, Instagram,
  Link as LinkIcon, Loader2, Mail, MessageCircle, Plus, Search, Send as SendIcon,
  Trash2, UserPlus, Zap,
} from 'lucide-react';
import api from '../lib/api';
import { useAuth } from '../context/AuthContext';
import { toast } from 'sonner';

const exampleWords = ['Price', 'Link', 'Shop'];

const TextArea = ({ className = '', ...props }) => (
  <textarea
    {...props}
    className={`w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm outline-none transition focus:border-slate-400 focus:ring-2 focus:ring-slate-900/10 ${className}`}
  />
);

const OptionRow = ({ active, title, children, onClick }) => (
  <div
    role="button"
    tabIndex={0}
    onClick={onClick}
    onKeyDown={(event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        onClick();
      }
    }}
    className={`w-full rounded-lg px-4 py-3 text-left transition ${
      active ? 'bg-white ring-2 ring-blue-500' : 'bg-slate-100 hover:bg-slate-50'
    }`}
  >
    <div className="flex items-start gap-3">
      {active ? (
        <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0 text-blue-600" />
      ) : (
        <Circle className="mt-0.5 h-5 w-5 shrink-0 text-slate-300" />
      )}
      <div className="min-w-0 flex-1">
        <div className="text-base font-medium text-slate-950">{title}</div>
        {children}
      </div>
    </div>
  </div>
);

const ToggleCard = ({ icon: Icon, title, checked, onChange, children }) => (
  <div className="rounded-lg bg-slate-100 p-4">
    <div className="flex items-center gap-3">
      {Icon && <Icon className="h-5 w-5 text-slate-500" />}
      <div className="flex-1 text-base font-medium text-slate-950">{title}</div>
      <Switch checked={checked} onCheckedChange={onChange} />
    </div>
    {checked && children && <div className="mt-3">{children}</div>}
  </div>
);

const AutomationPhonePreview = ({
  selectedMedia,
  postScope,
  keywordText,
  commentReply,
  openingDmText,
  openingDmButtonText,
  linkDmText,
  linkUrl,
  previewTab,
  setPreviewTab,
  accountName,
  accountAvatar,
}) => {
  const previewImage = selectedMedia?.thumbnail_url || selectedMedia?.media_url;
  const caption = selectedMedia?.caption || 'New post caption appears here';
  const handle = (accountName || 'instagram_account').replace(/^@/, '');
  const commentText = keywordText || 'Price';
  const Avatar = ({ size = 'h-9 w-9' }) => (
    <div className={`${size} shrink-0 overflow-hidden rounded-full bg-slate-200`}>
      {accountAvatar ? (
        <img src={accountAvatar} alt="" className="h-full w-full object-cover" />
      ) : (
        <div className="flex h-full w-full items-center justify-center bg-gradient-to-br from-pink-500 via-fuchsia-500 to-orange-400">
          <Instagram className="h-1/2 w-1/2 text-white" />
        </div>
      )}
    </div>
  );

  return (
    <div className="flex h-full flex-col bg-slate-50">
      <div className="px-4 pt-4 text-base font-medium text-slate-600 sm:px-8 sm:pt-8">Preview</div>
      <div className="flex flex-1 items-center justify-center px-3 py-4 sm:px-8 sm:py-8">
        <div className="w-[390px] max-w-full rounded-[3rem] bg-slate-950 p-4 shadow-2xl shadow-slate-300">
          <div className="overflow-hidden rounded-[2.25rem] bg-[#121212] text-white">
            <div className="flex h-12 items-center justify-between px-8 text-xs font-semibold">
              <span>2:34</span>
              <span className="h-1.5 w-16 rounded-full bg-white/10" />
              <span>LTE</span>
            </div>
            <div className="border-b border-white/5 px-5 pb-3 text-center">
              <div className="truncate text-xs font-bold uppercase text-white/45">{handle}</div>
              <div className="font-bold">Posts</div>
            </div>

            {previewTab === 'Post' && (
              <>
                <div className="flex items-center gap-3 px-4 py-3">
                  <Avatar />
                  <div className="min-w-0 flex-1 truncate text-sm font-bold">{handle}</div>
                  <div className="text-xl leading-none">...</div>
                </div>
                <div className="aspect-square bg-slate-900">
                  {previewImage ? (
                    <img src={previewImage} alt="" className="h-full w-full object-cover" />
                  ) : (
                    <div className="flex h-full w-full items-center justify-center bg-gradient-to-br from-slate-800 via-fuchsia-900 to-orange-700">
                      <Instagram className="h-16 w-16 text-white/70" />
                    </div>
                  )}
                </div>
                <div className="space-y-2 px-4 py-3">
                  <div className="flex items-center gap-3">
                    <Heart className="h-6 w-6" />
                    <MessageCircle className="h-6 w-6" />
                    <SendIcon className="h-6 w-6" />
                    <Bookmark className="ml-auto h-6 w-6" />
                  </div>
                  <div className="text-xs font-bold">14 likes</div>
                  <div className="line-clamp-2 text-xs">
                    <span className="font-bold">{handle}</span> {caption}
                  </div>
                  <div className="text-xs text-white/45">View all comments</div>
                  <div className="text-xs text-white/45">
                    {postScope === 'any' ? 'Any post or reel' : postScope === 'next' ? 'Next post or reel' : 'Selected post'}
                  </div>
                </div>
              </>
            )}

            {previewTab === 'Comments' && (
              <div className="min-h-[520px] px-4 py-5">
                <div className="mb-5 text-center font-bold">Comments</div>
                <div className="flex gap-3">
                  <div className="h-9 w-9 rounded-full bg-slate-200" />
                  <div>
                    <div className="rounded-2xl bg-white/10 px-3 py-2 text-sm">
                      <span className="font-bold">follower</span> {commentText}
                    </div>
                    {commentReply && (
                      <div className="mt-3 flex gap-2">
                        <Avatar size="h-7 w-7" />
                        <div className="rounded-2xl bg-white/10 px-3 py-2 text-sm">
                          <span className="font-bold">{handle}</span> {commentReply}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}

            {previewTab === 'DM' && (
              <div className="min-h-[520px] px-4 py-5">
                <div className="mb-5 text-center font-bold">DM</div>
                <div className="ml-auto max-w-[82%] rounded-2xl rounded-br-md bg-blue-600 px-3 py-2 text-sm">
                  {commentText}
                </div>
                {openingDmText && (
                  <div className="mt-4 max-w-[88%] rounded-2xl rounded-bl-md bg-white/10 px-3 py-2 text-sm">
                    {openingDmText}
                  </div>
                )}
                {openingDmButtonText && (
                  <div className="mt-2 max-w-[88%] rounded-xl border border-white/15 px-3 py-2 text-center text-sm font-semibold">
                    {openingDmButtonText}
                  </div>
                )}
                {(linkDmText || linkUrl) && (
                  <div className="mt-4 max-w-[88%] rounded-2xl rounded-bl-md bg-white/10 px-3 py-2 text-sm">
                    {linkDmText || 'Here is the link'}
                    {linkUrl && <div className="mt-2 text-blue-300">{linkUrl}</div>}
                  </div>
                )}
              </div>
            )}

            <div className="flex justify-around border-t border-white/5 py-3 text-white/80">
              <Instagram className="h-6 w-6" />
              <Search className="h-6 w-6" />
              <Plus className="h-6 w-6" />
              <MessageCircle className="h-6 w-6" />
            </div>
          </div>
        </div>
      </div>
      <div className="pb-8 text-center">
        <div className="inline-flex rounded-full bg-slate-200 p-1">
          {['Post', 'Comments', 'DM'].map(tab => (
            <button
              key={tab}
              type="button"
              onClick={() => setPreviewTab(tab)}
              className={`rounded-full px-5 py-1.5 text-sm font-medium transition ${
                previewTab === tab ? 'bg-white text-slate-950 shadow-sm' : 'text-slate-500'
              }`}
            >
              {tab}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
};

const Automations = () => {
  const { user } = useAuth();
  const [list, setList] = useState([]);
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all');
  const [loading, setLoading] = useState(true);
  const [instagramAccount, setInstagramAccount] = useState(null);

  const [builderOpen, setBuilderOpen] = useState(false);
  const [media, setMedia] = useState([]);
  const [mediaLoading, setMediaLoading] = useState(false);
  const [mediaError, setMediaError] = useState(null);
  const [mediaWarning, setMediaWarning] = useState(null);
  const [selectedMedia, setSelectedMedia] = useState(null);
  const [showAllMedia, setShowAllMedia] = useState(false);
  const [postScope, setPostScope] = useState('specific');
  const [match, setMatch] = useState('keyword');
  const [keyword, setKeyword] = useState('');
  const [replyUnderPost, setReplyUnderPost] = useState(false);
  const [commentReply, setCommentReply] = useState('Thanks. Check your DM.');
  const [openingDmEnabled, setOpeningDmEnabled] = useState(true);
  const [openingDmText, setOpeningDmText] = useState("Hey there. Thanks for your interest.\n\nClick below and I will send the link.");
  const [openingDmButtonText, setOpeningDmButtonText] = useState('Send me the link');
  const [followRequestEnabled, setFollowRequestEnabled] = useState(false);
  const [emailRequestEnabled, setEmailRequestEnabled] = useState(false);
  const [linkDmText, setLinkDmText] = useState('');
  const [linkButtonText, setLinkButtonText] = useState('Open link');
  const [linkUrl, setLinkUrl] = useState('');
  const [followUpEnabled, setFollowUpEnabled] = useState(false);
  const [followUpText, setFollowUpText] = useState('');
  const [processExistingComments, setProcessExistingComments] = useState(false);
  const [previewTab, setPreviewTab] = useState('Post');
  const [saving, setSaving] = useState(false);

  const refresh = async () => {
    setLoading(true);
    try {
      const { data } = await api.get('/automations');
      setList(data);
    } catch {
      toast.error('Failed to load automations');
    }
    setLoading(false);
  };

  useEffect(() => { refresh(); }, []);

  useEffect(() => {
    const loadInstagramProfile = async () => {
      if (!user?.instagramConnected) {
        setInstagramAccount(null);
        return;
      }
      setInstagramAccount({
        username: user.instagramHandle,
        profilePictureUrl: user.instagramProfilePictureUrl || user.avatar,
      });
      try {
        const { data } = await api.get('/instagram/profile');
        setInstagramAccount({
          username: data?.username || user.instagramHandle,
          profilePictureUrl: data?.profilePictureUrl || user.instagramProfilePictureUrl || user.avatar,
        });
      } catch {
        setInstagramAccount({
          username: user.instagramHandle,
          profilePictureUrl: user.instagramProfilePictureUrl || user.avatar,
        });
      }
    };
    loadInstagramProfile();
  }, [user]);

  const keywordList = useMemo(
    () => keyword.split(',').map(item => item.trim()).filter(Boolean),
    [keyword]
  );
  const previewAccountName = instagramAccount?.username || user?.instagramHandle || user?.username || 'instagram_account';
  const previewAccountAvatar = instagramAccount?.profilePictureUrl || user?.instagramProfilePictureUrl || user?.avatar || '';

  const filtered = list.filter(a =>
    (filter === 'all' || a.status === filter) &&
    (a.name || '').toLowerCase().includes(search.toLowerCase())
  );

  const resetBuilder = () => {
    setSelectedMedia(null);
    setShowAllMedia(false);
    setPostScope('specific');
    setMatch('keyword');
    setKeyword('');
    setReplyUnderPost(false);
    setCommentReply('Thanks. Check your DM.');
    setOpeningDmEnabled(true);
    setOpeningDmText("Hey there. Thanks for your interest.\n\nClick below and I will send the link.");
    setOpeningDmButtonText('Send me the link');
    setFollowRequestEnabled(false);
    setEmailRequestEnabled(false);
    setLinkDmText('');
    setLinkButtonText('Open link');
    setLinkUrl('');
    setFollowUpEnabled(false);
    setFollowUpText('');
    setProcessExistingComments(false);
    setPreviewTab('Post');
  };

  const openBuilder = async () => {
    resetBuilder();
    setBuilderOpen(true);
    setMedia([]);
    setMediaError(null);
    setMediaWarning(null);
    setMediaLoading(true);
    try {
      const { data } = await api.get('/instagram/media');
      const items = data?.media || data?.items || [];
      if (data?.ok === false) {
        setMedia([]);
        const errBody = data?.error?.body;
        setMediaError(typeof errBody === 'string' ? errBody : JSON.stringify(data?.error || data));
      } else {
        setMedia(items);
        if (items.length > 0) setSelectedMedia(items[0]);
        if (items.length === 0) {
          setMediaWarning(data?.warning || 'No Instagram media returned. Connect Instagram and publish a post first.');
        } else if (data?.warning) {
          setMediaWarning(data.warning);
        }
      }
    } catch (e) {
      setMediaError(e?.response?.data?.detail || e?.message || 'Failed to load posts. Connect Instagram first.');
      setMedia([]);
    }
    setMediaLoading(false);
  };

  const toggleStatus = async (a) => {
    const newStatus = a.status === 'active' ? 'paused' : 'active';
    setList(prev => prev.map(x => x.id === a.id ? { ...x, status: newStatus } : x));
    try {
      await api.patch(`/automations/${a.id}`, { status: newStatus });
    } catch {
      toast.error('Failed to update');
      refresh();
    }
  };

  const handleDelete = async (id) => {
    setList(prev => prev.filter(a => a.id !== id));
    try {
      await api.delete(`/automations/${id}`);
      toast.success('Deleted');
    } catch {
      toast.error('Failed');
      refresh();
    }
  };

  const canGoLive = () => {
    if (postScope === 'specific' && !selectedMedia) return false;
    if (match === 'keyword' && keywordList.length === 0) return false;
    if (replyUnderPost && !commentReply.trim()) return false;
    if (openingDmEnabled && !openingDmText.trim()) return false;
    if (!replyUnderPost && !openingDmEnabled && !linkDmText.trim() && !linkUrl.trim()) return false;
    return true;
  };

  const submit = async () => {
    if (!canGoLive()) return;
    setSaving(true);
    try {
      const dmPieces = [];
      if (openingDmEnabled && openingDmText.trim()) dmPieces.push(openingDmText.trim());
      if (openingDmButtonText.trim()) dmPieces.push(openingDmButtonText.trim());
      if (linkDmText.trim()) dmPieces.push(linkDmText.trim());
      if (linkUrl.trim()) dmPieces.push(linkUrl.trim());
      if (followUpEnabled && followUpText.trim()) dmPieces.push(followUpText.trim());
      const hasDm = openingDmEnabled || linkDmText.trim() || linkUrl.trim() || followRequestEnabled || emailRequestEnabled;

      const body = {
        post_scope: postScope,
        latest: postScope === 'next' || postScope === 'latest',
        mode: hasDm ? 'reply_and_dm' : 'reply_only',
        match,
        keyword: match === 'keyword' ? keywordList.join(', ') : '',
        keywords: match === 'keyword' ? keywordList : [],
        reply_under_post: replyUnderPost,
        comment_reply: replyUnderPost ? commentReply.trim() : '',
        dm_text: dmPieces.join('\n\n'),
        opening_dm_enabled: openingDmEnabled,
        opening_dm_text: openingDmEnabled ? openingDmText.trim() : '',
        opening_dm_button_text: openingDmButtonText.trim(),
        link_dm_text: linkDmText.trim(),
        link_button_text: linkButtonText.trim(),
        link_url: linkUrl.trim(),
        follow_request_enabled: followRequestEnabled,
        email_request_enabled: emailRequestEnabled,
        follow_up_enabled: followUpEnabled,
        follow_up_text: followUpText.trim(),
        processExistingComments,
      };

      if (postScope === 'specific' && selectedMedia) {
        body.media_id = selectedMedia.id;
        body.media_preview = {
          caption: selectedMedia.caption || '',
          thumbnail_url: selectedMedia.thumbnail_url || selectedMedia.media_url || '',
          media_type: selectedMedia.media_type || '',
        };
      }

      const { data } = await api.post('/automations/quick-comment-rule', body);
      setList(prev => [data, ...prev]);
      toast.success('Automation is live');
      setBuilderOpen(false);
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Failed to create automation');
    }
    setSaving(false);
  };

  if (builderOpen) {
    return (
      <div className="mx-auto max-w-7xl p-4 sm:p-6 lg:p-8 text-slate-950">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <h1 className="font-display text-3xl font-extrabold tracking-tight">Automations</h1>
            <p className="mt-1 text-slate-600">Create an Instagram comment automation inside your workspace.</p>
          </div>
          <div className="flex items-center gap-2">
            <Button variant="ghost" className="rounded-lg px-2" onClick={() => !saving && setBuilderOpen(false)}>
              <ArrowLeft className="mr-2 h-4 w-4" /> Back
            </Button>
            <Button
              onClick={submit}
              disabled={!canGoLive() || saving}
              className="rounded-lg bg-slate-950 px-5 text-white hover:bg-slate-800"
            >
              {saving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Go Live
            </Button>
          </div>
        </div>

        <div className="mt-6 grid gap-6 lg:grid-cols-[430px_minmax(0,1fr)]">
          <Card className="overflow-hidden rounded-2xl border-slate-100 bg-white shadow-sm">
            <div className="border-b border-slate-100 px-6 py-5">
              <div className="text-sm font-semibold uppercase tracking-wide text-slate-400">Comment automation</div>
              <div className="mt-1 text-lg font-bold">Build rule</div>
            </div>
            <div className="max-h-[calc(100vh-250px)] overflow-y-auto px-6 py-6">
            <section>
              <h2 className="text-2xl font-extrabold tracking-tight">When someone comments on</h2>
              <div className="mt-5 space-y-3">
                <OptionRow
                  active={postScope === 'specific'}
                  title="a specific post or reel"
                  onClick={() => setPostScope('specific')}
                >
                  <div className="mt-3">
                    {mediaError && (
                      <div className="rounded-lg border border-red-100 bg-red-50 p-3 text-xs text-red-700">
                        {mediaError}
                      </div>
                    )}
                    {!mediaError && mediaWarning && (
                      <div className="rounded-lg border border-amber-100 bg-amber-50 p-3 text-xs text-amber-800">
                        {mediaWarning}
                      </div>
                    )}
                    {mediaLoading ? (
                      <div className="flex items-center gap-2 py-6 text-sm text-slate-500">
                        <Loader2 className="h-4 w-4 animate-spin" /> Loading posts
                      </div>
                    ) : (
                      <div className="flex gap-2 overflow-x-auto pb-1">
                        {(showAllMedia ? media : media.slice(0, 8)).map(item => {
                          const thumb = item.thumbnail_url || item.media_url;
                          const selected = selectedMedia?.id === item.id;
                          return (
                            <button
                              key={item.id}
                              type="button"
                              onClick={(e) => {
                                e.stopPropagation();
                                setPostScope('specific');
                                setSelectedMedia(item);
                              }}
                              className={`h-24 w-24 shrink-0 overflow-hidden rounded-lg border-2 bg-slate-200 ${
                                selected ? 'border-blue-600' : 'border-transparent'
                              }`}
                            >
                              {thumb ? (
                                <img src={thumb} alt="" className="h-full w-full object-cover" />
                              ) : (
                                <div className="flex h-full w-full items-center justify-center">
                                  <Instagram className="h-7 w-7 text-slate-400" />
                                </div>
                              )}
                            </button>
                          );
                        })}
                      </div>
                    )}
                    {media.length > 8 && (
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          setShowAllMedia(value => !value);
                        }}
                        className="mt-4 text-sm font-semibold text-blue-600"
                      >
                        {showAllMedia ? 'Show Less' : 'Show All'}
                      </button>
                    )}
                  </div>
                </OptionRow>

                <OptionRow active={postScope === 'any'} title="any post or reel" onClick={() => setPostScope('any')} />
                <OptionRow active={postScope === 'next'} title="next post or reel" onClick={() => setPostScope('next')} />
              </div>
            </section>

            <section className="mt-10">
              <h2 className="text-2xl font-extrabold tracking-tight">And this comment has</h2>
              <div className="mt-5 space-y-3">
                <OptionRow
                  active={match === 'keyword'}
                  title="a specific word or words"
                  onClick={() => setMatch('keyword')}
                >
                  <div className="mt-3 space-y-3">
                    <Input
                      value={keyword}
                      onChange={e => setKeyword(e.target.value)}
                      placeholder="Enter a word or multiple"
                      className="h-11 rounded-lg bg-white"
                      onClick={e => e.stopPropagation()}
                    />
                    <div className="text-sm text-slate-500">Use commas to separate words</div>
                    <div className="flex flex-wrap items-center gap-2 text-sm text-slate-500">
                      <span>For example:</span>
                      {exampleWords.map(word => (
                        <button
                          key={word}
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation();
                            const current = new Set(keywordList.map(k => k.toLowerCase()));
                            if (!current.has(word.toLowerCase())) {
                              setKeyword([...keywordList, word].join(', '));
                            }
                          }}
                          className="rounded-full border border-blue-200 bg-blue-50 px-3 py-1 text-blue-700"
                        >
                          {word}
                        </button>
                      ))}
                    </div>
                  </div>
                </OptionRow>

                <OptionRow active={match === 'any'} title="any word" onClick={() => setMatch('any')} />

                <ToggleCard
                  title="reply to their comments under the post"
                  checked={replyUnderPost}
                  onChange={setReplyUnderPost}
                  icon={MessageCircle}
                >
                  <Input
                    value={commentReply}
                    onChange={e => setCommentReply(e.target.value)}
                    placeholder="Write a public reply"
                    className="h-11 rounded-lg bg-white"
                  />
                </ToggleCard>
              </div>
            </section>

            <section className="mt-10">
              <h2 className="text-2xl font-extrabold tracking-tight">They will get</h2>
              <div className="mt-5 space-y-3">
                <ToggleCard
                  title="an opening DM"
                  checked={openingDmEnabled}
                  onChange={setOpeningDmEnabled}
                  icon={SendIcon}
                >
                  <TextArea
                    value={openingDmText}
                    onChange={e => setOpeningDmText(e.target.value)}
                    rows={5}
                    placeholder="Write the first DM"
                  />
                  <Input
                    value={openingDmButtonText}
                    onChange={e => setOpeningDmButtonText(e.target.value)}
                    placeholder="Button text"
                    className="mt-3 h-11 rounded-lg bg-white"
                  />
                </ToggleCard>

                <ToggleCard
                  title="a DM asking to follow you before they get the link"
                  checked={followRequestEnabled}
                  onChange={setFollowRequestEnabled}
                  icon={UserPlus}
                />

                <ToggleCard
                  title="a DM asking for their email"
                  checked={emailRequestEnabled}
                  onChange={setEmailRequestEnabled}
                  icon={Mail}
                />
              </div>
            </section>

            <section className="mt-10">
              <h2 className="text-2xl font-extrabold tracking-tight">And then, they will get</h2>
              <div className="mt-5 space-y-3">
                <div className="rounded-lg bg-slate-100 p-4">
                  <div className="mb-3 flex items-center gap-3 text-base font-medium">
                    <LinkIcon className="h-5 w-5 text-slate-500" /> a DM with a link
                  </div>
                  <TextArea
                    value={linkDmText}
                    onChange={e => setLinkDmText(e.target.value)}
                    rows={4}
                    placeholder="Write a message"
                  />
                  <Input
                    value={linkUrl}
                    onChange={e => setLinkUrl(e.target.value)}
                    placeholder="https://example.com"
                    className="mt-3 h-11 rounded-lg bg-white"
                  />
                  <Input
                    value={linkButtonText}
                    onChange={e => setLinkButtonText(e.target.value)}
                    placeholder="Button text"
                    className="mt-3 h-11 rounded-lg bg-white"
                  />
                </div>

                <ToggleCard
                  title="a follow up DM if they don't click the link"
                  checked={followUpEnabled}
                  onChange={setFollowUpEnabled}
                  icon={SendIcon}
                >
                  <TextArea
                    value={followUpText}
                    onChange={e => setFollowUpText(e.target.value)}
                    rows={3}
                    placeholder="Write a follow up"
                  />
                </ToggleCard>

                <div className="rounded-lg border border-slate-200 bg-white p-4">
                  <label className="flex items-start gap-3">
                    <Checkbox
                      checked={processExistingComments}
                      onCheckedChange={v => setProcessExistingComments(Boolean(v))}
                      className="mt-0.5"
                    />
                    <span>
                      <span className="block text-sm font-semibold">Also process existing comments</span>
                      <span className="block text-xs text-slate-500">
                        Leave unchecked to only respond to comments created after this automation goes live.
                      </span>
                    </span>
                  </label>
                  {processExistingComments && (
                    <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                      This may send replies/DMs to comments that already exist on this post.
                    </div>
                  )}
                </div>
              </div>
            </section>
            </div>
          </Card>

          <Card className="min-w-0 overflow-hidden rounded-2xl border-slate-100 bg-white shadow-sm">
            <AutomationPhonePreview
              selectedMedia={selectedMedia}
              postScope={postScope}
              keywordText={keywordList[0] || ''}
              commentReply={replyUnderPost ? commentReply : ''}
              openingDmText={openingDmEnabled ? openingDmText : ''}
              openingDmButtonText={openingDmEnabled ? openingDmButtonText : ''}
              linkDmText={linkDmText}
              linkUrl={linkUrl}
              previewTab={previewTab}
              setPreviewTab={setPreviewTab}
              accountName={previewAccountName}
              accountAvatar={previewAccountAvatar}
            />
          </Card>
        </div>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-6xl p-4 sm:p-6 lg:p-8">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="font-display text-3xl font-extrabold tracking-tight">Automations</h1>
          <p className="mt-1 text-slate-600">Build Instagram comment automations for new comments only.</p>
        </div>
        <Button onClick={openBuilder} className="rounded-lg bg-slate-900 text-white hover:bg-slate-800">
          <Plus className="mr-1.5 h-4 w-4" /> Create Automation
        </Button>
      </div>

      <div className="mt-6 flex flex-wrap items-center gap-3">
        <div className="relative min-w-[240px] max-w-sm flex-1">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
          <Input
            placeholder="Search automations..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="h-10 rounded-lg bg-white pl-9"
          />
        </div>
        <div className="flex gap-1 rounded-lg border border-slate-200 bg-white p-1">
          {['all', 'active', 'paused', 'draft'].map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`rounded-md px-4 py-1.5 text-sm font-medium capitalize transition-colors ${
                filter === f ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'
              }`}
            >
              {f}
            </button>
          ))}
        </div>
      </div>

      <div className="mt-6 grid gap-3">
        {filtered.map(a => {
          const thumb = a.media_preview?.thumbnail_url;
          const scopeLabel = a.post_scope === 'any'
            ? 'Any post'
            : a.latest ? 'Next/latest post' : (a.media_preview?.caption?.slice(0, 40) || a.media_id?.slice(0, 10) || '');
          const keywordLabel = a.match === 'keyword' && a.keyword ? `keywords "${a.keyword}"` : 'any word';
          const modeLabel = a.mode === 'reply_only' ? 'Reply only' : 'Reply + DM';
          return (
            <Card key={a.id} className="rounded-lg border-slate-100 p-4 transition-shadow hover:shadow-md">
              <div className="flex flex-wrap items-center gap-4">
                <div className="flex h-14 w-14 shrink-0 items-center justify-center overflow-hidden rounded-lg bg-gradient-to-br from-pink-500 via-fuchsia-500 to-orange-400">
                  {thumb ? <img src={thumb} alt="" className="h-full w-full object-cover" /> : <Zap className="h-6 w-6 text-white" />}
                </div>
                <div className="min-w-[200px] flex-1">
                  <div className="font-semibold">{a.name}</div>
                  <div className="mt-0.5 text-xs text-slate-500">
                    {scopeLabel} - {modeLabel} - {keywordLabel}
                  </div>
                  {a.activationStartedAt && (
                    <div className="mt-1 text-xs text-slate-400">
                      Active since {new Date(a.activationStartedAt).toLocaleString()}
                    </div>
                  )}
                </div>
                <div className="hidden gap-6 text-sm md:flex">
                  <div>
                    <div className="text-xs text-slate-500">Fired</div>
                    <div className="font-bold">{(a.sent || 0).toLocaleString()}</div>
                  </div>
                </div>
                <Badge className={`rounded-full ${
                  a.status === 'active'
                    ? 'border-emerald-100 bg-emerald-50 text-emerald-700'
                    : a.status === 'paused'
                      ? 'border-amber-100 bg-amber-50 text-amber-700'
                      : 'border-slate-200 bg-slate-100 text-slate-600'
                }`}>
                  {a.status}
                </Badge>
                <Switch checked={a.status === 'active'} onCheckedChange={() => toggleStatus(a)} />
                <Button
                  onClick={() => handleDelete(a.id)}
                  variant="ghost"
                  size="icon"
                  className="rounded-lg text-red-500 hover:bg-red-50 hover:text-red-600"
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            </Card>
          );
        })}

        {!loading && filtered.length === 0 && (
          <Card className="rounded-lg border-slate-100 p-12 text-center">
            <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-lg bg-slate-100">
              <Zap className="h-6 w-6 text-slate-400" />
            </div>
            <h3 className="font-display mt-4 text-lg font-bold">No automations yet</h3>
            <p className="mt-1 text-sm text-slate-500">Create your first Instagram comment automation.</p>
          </Card>
        )}
      </div>
    </div>
  );
};

export default Automations;
