# Instagram Multi-Account Flow Checklist

Use this for production smoke after deploying multi-account routing changes. Do not record access tokens, full webhook payloads, or full message bodies in notes.

## Account A comments on Account B

1. From Account A, comment once on a post owned by Account B.
   - Expected: Account B replies publicly once.
   - Expected: Account B sends the opening DM once.
   - Expected: the comment-flow session is stored under Account B.
2. From Account A, comment again on the same Account B post.
   - Expected: no second public reply.
   - Expected: no second opening DM.
   - Expected: logs show duplicate skipped for the same commenter, post, rule, and account.
3. Click the next button in the DM from Account B.
   - Expected: Account B sends the next step.
   - Expected: follow confirmation continues under Account B.
   - Expected: follow verification checks Account B.

## Account B comments on Account A

Repeat the same sequence with Account B commenting on Account A.

Expected:
- Account A rule starts.
- Account A sends the public reply and opening DM.
- Account B rule does not run just because Account B is the commenter.
- Button/postback continuation stays under Account A.

## Replay and pacing checks

1. Replay the same webhook event if available.
   - Expected: duplicate webhook delivery is skipped.
2. Send comments from multiple different commenters.
   - Expected: each different commenter can start a flow.
   - Expected: bursts are paced safely.
3. Confirm sibling-account DM automation loop guard.
   - Expected: comment-to-DM opening between sibling accounts is allowed.
   - Expected: DM automation ping-pong remains blocked.
