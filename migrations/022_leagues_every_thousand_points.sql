-- Leagues are now 1,000 points wide, starting from the 1,000 everybody
-- begins on: Bronze up to 2,000, Silver from 2,000, Gold from 3,000,
-- Platinum from 4,000, Diamond from 5,000 and Ace from 6,000 (leagueOfPoints
-- in battles.go).
--
-- The league column is only rewritten when a battle is decided, so without
-- this everybody would keep the league the old, much closer lines gave them
-- — somebody on 1,100 points would still say Silver — until their next
-- battle. This works each league out again from the points with the new
-- lines.
--
-- Only people who have had a battle decided: everybody else has not earned
-- a league yet and is left exactly as they are.
UPDATE users
   SET league = CASE
           WHEN rating >= 6000 THEN 'Ace'
           WHEN rating >= 5000 THEN 'Diamond'
           WHEN rating >= 4000 THEN 'Platinum'
           WHEN rating >= 3000 THEN 'Gold'
           WHEN rating >= 2000 THEN 'Silver'
           ELSE 'Bronze'
       END
 WHERE COALESCE(wins, 0) + COALESCE(losses, 0) + COALESCE(draws, 0) > 0;
