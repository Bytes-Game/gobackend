-- ════════════════════════════════════════════════════════════════════════════
-- REPORTS ON THEIR OWN PENALISE; THEY DO NOT TAKE A VIDEO DOWN
-- ════════════════════════════════════════════════════════════════════════════
--
-- The owner asked for this: with so few videos on the app, three reports
-- alone should not be able to remove one. They cost its owner rating points
-- instead, and the video stays up. See offtopic.go.
--
-- When the owner was charged for reports on this video. NULL = never. It is
-- what makes that charge happen once per video, however many more reports
-- come in, and what stops them paying the same penalty again if the video is
-- later taken down after all.
ALTER TABLE challenges          ADD COLUMN IF NOT EXISTS report_penalty_at TIMESTAMPTZ;
ALTER TABLE challenge_responses ADD COLUMN IF NOT EXISTS report_penalty_at TIMESTAMPTZ;
