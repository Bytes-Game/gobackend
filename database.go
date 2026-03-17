package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

// db is the PostgreSQL connection pool shared across the application.
var db *sql.DB

// ---------------------------------------------------------------------------------
// Initialisation
// ---------------------------------------------------------------------------------

// InitDatabase connects to PostgreSQL, runs schema migrations, and seeds
// sample data if the tables are empty.
func InitDatabase() {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	runMigrations()
	seedIfEmpty()
}

// runMigrations creates all required tables idempotently.
func runMigrations() {
	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id          SERIAL PRIMARY KEY,
		username	VARCHAR(50) UNIQUE NOT NULL,
		password	VARCHAR(255) NOT NULL,
		full_name	VARCHAR(100) DEFAULT '',
		wins		INT DEFAULT 0,
		losses		INT DEFAULT 0,
		league		VARCHAR(20) DEFAULT 'Bronze'
	);

	CREATE TABLE IF NOT EXISTS follows (
		follower_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		following_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at	 TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (follower_id, following_id)
	);

	CREATE TABLE IF NOT EXISTS posts (
		id			  SERIAL PRIMARY KEY,
		author_id	  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		type		  VARCHAR(10) NOT NULL DEFAULT 'image',
		content_url   TEXT DEFAULT '',
		thumbnail_url TEXT DEFAULT '',
		caption		  TEXT DEFAULT '',
		views		  INT DEFAULT 0,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS post_likes (
		post_id	   INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
		user_id	   INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (post_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS comments(
		id		   SERIAL PRIMARY KEY,
		post_id	   INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
		author_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		text	   TEXT NOT NULL,
		created_at TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenges(
		id 		  	  SERIAL PRIMARY KEY,
		creator_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		video_url     TEXT DEFAULT '',
		thumbnail_url TEXT DEFAULT '',
		prefix 		  VARCHAR(100) NOT NULL DEFAULT '',
		subject       VARCHAR(100) NOT NULL DEFAULT '',
		visibility    VARCHAR(10) NOT NULL DEFAULT 'arena',
		status        VARCHAR(20) NOT NULL DEFAULT 'open',
		views         INT DEFAULT 0,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenge_visible_to (
		challenge_id INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		user_id      INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		PRIMARY KEY (challenge_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS challenge_responses (
		id            SERIAL PRIMARY KEY,
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		responder_id  INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		video_url     TEXT DEFAULT '',
		thumbnail_url TEXT DEFAULT '',
		views         INT DEFAULT 0,
		created_at    TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS challenge_likes (
		challenge_id  INT NOT NULL REFERENCES challenges(id) ON DELETE CASCADE,
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY  (challenge_id, user_id)
	);

	CREATE TABLE IF NOT EXISTS challenge_response_likes (
		response_id   INT NOT NULL REFERENCES challenge_responses(id) ON DELETE CASCADE,
		user_id       INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		created_at    TIMESTAMPTZ DEFAULT NOW(),
		PRIMARY KEY (response_id, user_id)
	);
	`

	if _, err := db.Exec(schema); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}
	log.Println("Database migrations completed")
}

// --------------------------------------------------------------------------
// User CRUD
// --------------------------------------------------------------------------

// readUser scans basic user columns and enriches with follower count + following list
// Use for single-user lookups (3 queries total).
func readUser(id int, username, pw, fullName string, wins, losses int, league string) User {
	u := User{
		ID:       strconv.Itoa(id),
		Username: username,
		password: pw,
		FullName: fullName,
		Wins:     wins,
		Losses:   losses,
		League:   league,
	}

	// Followers count
	_ = db.QueryRow(`SELECT COUNT(*) FROM follows WHERE following_id = $1`, id).Scan(&u.Followers)

	// Following list (string IDs for JSON compatibility)
	rows, err := db.Query(`SELECT following_id FROM follows WHERE follower_id = $1`, id)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var fid int
			if rows.Scan(&fid) == nil {
				u.FollowingList = append(u.FollowingList, strconv.Itoa(fid))
			}
		}
	}
	if u.FollowingList == nil {
		u.FollowingList = []string{}
	}
	return u
}

// enrichUsers batch-populates Followers and FollowingList for a slice of users
// using only one extra DB query (much faster for lists).
func enrichUsers(users []User) {
	if len(users) == 0 {
		return
	}

	rows, err := db.Query(`SELECT follower_id, following_id FROM follows`)
	if err != nil {
		return
	}
	defer rows.Close()

	followingMap := make(map[string][]string) // follower → []following
	followerCount := make(map[string]int)     // user → count of followers

	for rows.Next() {
		var fid, tid int
		if rows.Scan(&fid, &tid) == nil {
			fidStr := strconv.Itoa(fid)
			tidStr := strconv.Itoa(tid)
			followingMap[fidStr] = append(followingMap[fidStr], tidStr)
			followerCount[tidStr]++
		}
	}

	for i := range users {
		users[i].Followers = followerCount[users[i].ID]
		users[i].FollowingList = followingMap[users[i].ID]
		if users[i].FollowingList == nil {
			users[i].FollowingList = []string{}
		}
	}
}

// GetuserByUsername returns a fully enriched user, looked up by username.
func GetUserByUsername(username string) (User, bool) {
	var id, wins, losses int
	var uname, pw, fullName, league string
	err := db.QueryRow(
		`SELECT id, username, password, full_name, wins, losses, league FROM users WHERE username = $1`,
		username,
	).Scan(&id, &uname, &pw, &fullName, &wins, &losses, &league)
	if err != nil {
		return User{}, false
	}
	return readUser(id, uname, pw, fullName, wins, losses, league), true
}

// GetUserByID returns a fully enriched user, looked up by string ID.
func GetUserByID(idStr string) (User, bool) {
	idInt, err := strconv.Atoi(idStr)
	if err != nil {
		return User{}, false
	}
	var wins, losses int
	var uname, pw, fullName, league string
	err = db.QueryRow(
		`SELECT id, username, password, full_name, wins, losses, league FROM users WHERE id = $1`,
		idInt,
	).Scan(&idInt, &uname, &pw, &fullName, &wins, &losses, &league)
	if err != nil {
		return User{}, false
	}
	return readUser(idInt, uname, pw, fullName, wins, losses, league), true
}

// UserExists checks whether a username is already taken.
func UserExists(username string) bool {
	var exists bool
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM users WHERE username = $1`, username).Scan(&exists)
	return exists
}

// IsValidUser checks credentials (plain-text comparison - hash in production).
func IsValidUser(username, password string) bool {
	var exists bool
	db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM users WHERE username = $1 AND password = $2)`,
		username, password,
	).Scan(&exists)
	return exists
}

// GetAllUsers returns every user, batch-enriched with follow data.
func GetAllUsers() []User {
	rows, err := db.Query(`SELECT id, username, password, full_name, wins, losses, league FROM users ORDER BY id`)
	if err != nil {
		log.Printf("GetAllUsers error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []User
	for rows.Next() {
		var id, wins, losses int
		var uname, pw, fullName, league string
		if rows.Scan(&id, &uname, &pw, &fullName, &wins, &losses, &league) == nil {
			result = append(result, User{
				ID:       strconv.Itoa(id),
				Username: uname,
				password: pw,
				FullName: fullName,
				Wins:     wins,
				Losses:   losses,
				League:   league,
			})
		}
	}

	enrichUsers(result)
	return result
}

// --------------------------------------------------------------------------------
// Post CRUD
// --------------------------------------------------------------------------------

// postBaseQuery is the common SELECT used for all post retrievals.
// It joins author info and aggregates likes + comments in a single scan.
const postBaseQuery = `
SELECT p.id, p.author_id, u.username, u.league,
	   p.type,
	   COALESCE(p.content_url,   '') AS content_url,
	   COALESCE(p.thumbnail_url, '') AS thumbnail_url,
	   p.caption, p.views,
	   COALESCE(lc.cnt, 0) AS likes,
	   COALESCE(cc.cnt, 0) AS comment_count,
	   p.created_at
FROM posts p
JOIN users u ON p.author_id = u.id
LEFT JOIN (SELECT post_id, COUNT(*) AS cnt FROM post_likes GROUP BY post_id) lc ON lc.post_id = p.id
LEFT JOIN (SELECT post_id, COUNT(*) AS cnt FROM comments GROUP BY post_id) cc ON cc.post_id = p.id`

// queryPosts executes a full post query string and returns enriched Post structs
// including the LikedBy list (batch-fetched).
func queryPosts(query string, args ...interface{}) []Post {
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("queryPosts error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []Post
	var postIDs []int

	for rows.Next() {
		var id, authorID, views, likes, comments int
		var username, league, postType, contentURL, thumbnailURL, caption string
		var createdAt time.Time

		if rows.Scan(&id, &authorID, &username, &league,
			&postType, &contentURL, &thumbnailURL,
			&caption, &views, &likes, &comments, &createdAt) == nil {

			result = append(result, Post{
				ID:             strconv.Itoa(id),
				AuthorID:       strconv.Itoa(authorID),
				AuthorUsername: username,
				AuthorLeague:   league,
				Type:           postType,
				ContentURL:     contentURL,
				ThumbnailURL:   thumbnailURL,
				Caption:        caption,
				Views:          views,
				Likes:          likes,
				Comments:       comments,
				CreatedAt:      createdAt.UTC().Format(time.RFC3339),
			})
			postIDs = append(postIDs, id)
		}
	}

	// Batch-fetch LikedBy for every returned post (1 extra query)
	if len(postIDs) > 0 {
		likedByMap := getLikedByMap(postIDs)
		for i := range result {
			if lb, ok := likedByMap[result[i].ID]; ok {
				result[i].LikedBy = lb
			}
		}
	}

	return result
}

// getLikedByMap returns a map[postip==IDStr] -> []userIDStr for the given post IDs.
func getLikedByMap(postIDs []int) map[string][]string {
	result := make(map[string][]string)
	if len(postIDs) == 0 {
		return result
	}

	placeholders := ""
	args := make([]interface{}, len(postIDs))
	for i, id := range postIDs {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	rows, err := db.Query(
		fmt.Sprintf(`SELECT post_id, user_id FROM post_likes WHERE post_id IN (%s)`, placeholders),
		args...,
	)
	if err != nil {
		return result
	}
	defer rows.Close()

	for rows.Next() {
		var pid, uid int
		if rows.Scan(&pid, &uid) == nil {
			pidStr := strconv.Itoa(pid)
			result[pidStr] = append(result[pidStr], strconv.Itoa(uid))
		}
	}
	return result
}

// GetAllPosts returns every post, newest first.
func GetAllPosts() []Post {
	return queryPosts(postBaseQuery + ` ORDER BY p.created_at DESC`)
}

// GetPostsPaginated returns a page of posts and whether more pages exist.
func GetPostsPaginated(page, limit int) ([]Post, bool) {
	offset := (page - 1) * limit
	// Fetch one extra row to check if there's a next page
	posts := queryPosts(postBaseQuery+` ORDER BY p.created_at DESC LIMIT $1 OFFSET $2`, limit+1, offset)
	hasMore := len(posts) > limit
	if hasMore {
		posts = posts[:limit]
	}
	return posts, hasMore
}

// GetPostsByUserID returns all posts by a given author.
func GetPostsByUserID(userID string) []Post {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return nil
	}
	return queryPosts(postBaseQuery+` WHERE p.author_id = $1 ORDER BY p.created_at DESC`, uid)
}

// GetPostByID returns a single post.
func GetPostByID(idStr string) (Post, bool) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return Post{}, false
	}
	posts := queryPosts(postBaseQuery+` WHERE p.id = $1`, id)
	if len(posts) == 0 {
		return Post{}, false
	}
	return posts[0], true
}

// ToggleLike adds or removes a like and returns (liked, newCount, UpdatedPost).
func ToggleLike(postID, userID string) (bool, int, Post) {
	pid, err1 := strconv.Atoi(postID)
	uid, err2 := strconv.Atoi(userID)
	if err1 != nil || err2 != nil {
		return false, 0, Post{}
	}

	var exists bool
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM post_likes WHERE post_id=$1 AND user_id=$2)`, pid, uid).Scan(&exists)

	if exists {
		db.Exec(`DELETE FROM post_likes WHERE post_id=$1 AND user_id=$2`, pid, uid)
	} else {
		db.Exec(`INSERT INTO post_likes (post_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, pid, uid)
	}

	post, found := GetPostByID(postID)
	if !found {
		return false, 0, Post{}
	}
	return !exists, post.Likes, post
}

// ---------------------------------------------------------------------------
// Comment CRUD
// ---------------------------------------------------------------------------

// AddComment inserts a new comment and returns the created Comment struct.
func AddComment(postID, authorID, authorUsername, text string) Comment {
	pid, err1 := strconv.Atoi(postID)
	uid, err2 := strconv.Atoi(authorID)
	if err1 != nil || err2 != nil {
		return Comment{}
	}

	var id int
	var createdAt time.Time
	err := db.QueryRow(
		`INSERT INTO comments (post_id, author_id, text) VALUES ($1,$2,$3) RETURNING id, created_at`,
		pid, uid, text,
	).Scan(&id, &createdAt)
	if err != nil {
		log.Printf("AddComment error: %v", err)
		return Comment{}
	}

	return Comment{
		ID:             strconv.Itoa(id),
		PostID:         postID,
		AuthorID:       authorID,
		AuthorUsername: authorUsername,
		Text:           text,
		CreatedAt:      createdAt.UTC().Format(time.RFC3339),
	}
}

// GetComments returns all comments for a post, with author usernames resolved via JOIN
func GetComments(postID string) []Comment {
	pid, err := strconv.Atoi(postID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT c.id, c.post_id, c.author_id, u.username, c.text, c.created_at
		FROM comments c
		JOIN users u ON c.author_id = u.id
		WHERE c.post_id = $1
		ORDER BY c.created_at ASC`, pid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []Comment
	for rows.Next() {
		var cid, postIDInt, authorID int
		var username, txt string
		var createdAt time.Time
		if rows.Scan(&cid, &postIDInt, &authorID, &username, &txt, &createdAt) == nil {
			result = append(result, Comment{
				ID:             strconv.Itoa(cid),
				PostID:         strconv.Itoa(postIDInt),
				AuthorID:       strconv.Itoa(authorID),
				AuthorUsername: username,
				Text:           txt,
				CreatedAt:      createdAt.UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// -----------------------------------------------------------------------------
// Seed data (runs once when tables are empty)
// -----------------------------------------------------------------------------

func seedIfEmpty() {
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count)
	if count > 0 {
		log.Println("Database already has data - skipping seed")
		return
	}

	log.Println("Seeding database with initial data...")
	seedUsers()
	seedFollows()
	seedPosts()
	seedComments()
	seedChallenges()
	log.Println("Seeding complete")
}

func seedUsers() {
	type su struct {
		username, password, fullName, league string
		wins, losses                         int
	}
	data := []su{
		{"player1", "pass1", "Player One", "Gold", 32, 18},
		{"player2", "pass2", "Player Two", "Diamond", 120, 45},
		{"player3", "pass3", "Player Three", "Bronze", 10, 5},
		{"shadowstrike", "pass4", "Shadow Strike", "Platinum", 95, 30},
		{"blazerunner", "pass5", "Blaze Runner", "Silver", 55, 40},
		{"stormchaser", "pass6", "Storm Chaser", "Gold", 78, 22},
		{"frostbyte", "pass7", "Frost Byte", "Dianond", 140, 50},
		{"nightowl", "pass8", "Night Owl", "Bronze", 15, 10},
		{"thunderbolt", "pass9", "Thunder Bolt", "Silver", 62, 38},
		{"cyberking", "pass10", "Cyber King", "Platinum", 100, 25},
	}
	for _, u := range data {
		db.Exec(
			`INSERT INTO users (username, password, full_name, wins, losses, league)
			 VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,
			u.username, u.password, u.fullName, u.wins, u.losses, u.league,
		)
	}
}

func seedFollows() {
	fm := map[int][]int{
		1: {2, 3, 4}, 2: {1, 5}, 3: {1, 2}, 4: {1, 2, 7},
		5: {2, 4, 6}, 6: {1, 5, 7}, 7: {2, 4, 6, 8},
		8: {1, 7}, 9: {4, 7, 10}, 10: {1, 2, 4, 7},
	}
	for f, list := range fm {
		for _, t := range list {
			db.Exec(`INSERT INTO follows (follower_id, following_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, f, t)
		}
	}
}

func seedPosts() {
	type sp struct {
		authorID                                    int
		postType, contentURL, thumbnailURL, caption string
		views                                       int
		createdAt                                   string
	}
	data := []sp{
		{2, "video", "https://cdn.pixabay.com/video/2026/02/09/333600_small.mp4", "https://cdn.pixabay.com/video/2026/02/09/333600_small.jpg", "Diamond league gameplay - watch and learn", 5400, "2026-02-20T14:00:00Z"},
		{7, "video", "", "", "Frozen intime New combo showcase!", 8800, "2026-02-20T13:30:00Z"},
		{1, "video", "https://cdn.pixabay.com/video/2026/02/15/334716_small.mp4", "https://cdn.pixabay.com/video/2026/02/15/334716_small.jpg", "Finally broke my personal record", 1200, "2026-02-20T12:45:00Z"},
		{10, "image", "", "", "New setup reveal Ready to dominate!", 3200, "2026-02-20T11:15:00Z"},
		{4, "video", "", "", "Shadow techniques vol.3 - the comeback is real ", 4100, "2026-02-20T10:30:00Z"},
		{5, "video", "", "", "Speed run challenge accepted!", 2100, "2026-02-20T09:00:00Z"},
		{6, "video", "", "", "Storm surge combo into triple elimination", 3800, "2026-02-19T22:00:00Z"},
		{3, "image", "https://pixabay.com/get/g1cb3fb5e78308321688cd47550266beec73006700f8fd3f16549b183bd601669b6128582de0d1c92a4800ae0109174150e040eaeba050fdae70980c53ef8761c_1280.jpg", "", "Just started competing - wish me luck! ", 800, "2026-02-19T20:30:00Z"},
		{9, "video", "", "", "Thunder strike compilation ", 2600, "2026-02-19T18:45:00Z"},
		{8, "video", "", "", "Late-night practice session ", 650, "2026-02-19T17:00:00Z"},
		{2, "video", "https://cdn.pixabay.com/video/2026/01/09/326739_small.mp4", "https://cdn.pixabay.com/video/2026/01/09/326739_small.jpg", "1v1 challenge against shadowstrike - who wins?", 9200, "2026-02-19T15:30:00Z"},
		{4, "image", "", "", "Platinum badge unlocked!", 5100, "2026-02-19T14:00:00Z"},
		{7, "video", "", "", "Tutorial: Advanced freeze frame technique", 4200, "2026-02-19T12:15:00Z"},
		{1, "video", "https://cdn.pixabay.com/video/2026/01/19/328740_small.mp4", "https://cdn.pixabay.com/video/2026/01/19/328740_small.jpg", "Gold league highlights - best plays this week ", 1800, "2026-02-19T10:30:00Z"},
		{10, "video", "", "", "AI - assisted training results are insane", 7500, "2026-02-19T08:00:00Z"},
		{6, "video", "", "", "Road to Platinum - day 45 of the grind ", 2400, "2026-02-18T23:00:00Z"},
		{5, "image", "", "", "New controller just dropped ", 1600, "2026-02-18T21:00:00Z"},
		{9, "video", "", "", "My best clutch moment yet - 1 HP survival!!", 4800, "2026-02-18T19:00:00Z"},
		{3, "video", "https://cdn.pixabay.com/video/2026/01/19/328740_small.mp4", "https://cdn.pixabay.com/video/2026/01/19/328740_small.jpg", "Learning from the pros Watch me improve!", 500, "2026-02-18T17:30:00Z"},
		{8, "video", "", "", "First win of the season ", 900, "2026-02-18T16:00:00Z"},
		{2, "video", "https://cdn.pixabay.com/video/2026/02/15/334716_small.mp4", "https://cdn.pixabay.com/video/2026/02/15/334716_small.jpg", "How I got to Diamond in 30 days - full guide", 12000, "2026-02-18T14:00:00Z"},
		{7, "image", "", "", "New character skin unlocked - thoughts?", 3100, "2026-02-18T12:00:00Z"},
		{4, "video", "", "", "Top 5 mistakes beginners make (avoid these!)", 5600, "2026-02-18T10:00:00Z"},
		{10, "video", "", "", "Challenge accepted: 24-hour win streak attempt ", 8900, "2026-02-18T08:00:00Z"},
		{6, "video", "", "", "Storm vs Blaze - epic rivalry match recap ", 3600, "2026-02-17T22:00:00Z"},
		{1, "image", "https://pixabay.com/get/g6d2fb7b4dde02b4febf151624eaf6ee7f096bc81be660c3da10483538d9a3386d3a89ce0a6cfb52722f4c7e824fd261614b56eddbf31da7d1d4f96fcab32dc5e_640.jpg", "", "My journey from Bronze to Gold in one month", 2200, "2026-02-17T18:00:00Z"},
		{5, "video", "", "", "Speed kills - fastest elimination compilation", 2000, "2026-02-17T15:00:00Z"},
		{9, "video", "", "", "Team challenge highlights with frostbyte!", 1700, "2026-02-17T12:00:00Z"},
	}
	for _, p := range data {
		t, _ := time.Parse(time.RFC3339, p.createdAt)
		db.Exec(
			`INSERT INTO posts (author_id, type, content_url, thumbnail_url, caption, views, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			p.authorID, p.postType, p.contentURL, p.thumbnailURL, p.caption, p.views, t,
		)
	}
}

func seedComments() {
	type sc struct {
		postID, authorID int
		text, createdAt  string
	}
	data := []sc{
		{1, 1, "Insane gameplay! Teach me your ways ", "2026-02-20T14:30:00Z"},
		{1, 4, "Diamond players are built different", "2026-02-20T14:45:00Z"},
		{1, 7, "GG, let's run duos sometime", "2026-02-20T15:00:00Z"},
		{2, 2, "That combo is nutty", "2026-02-20T13:45:00Z"},
		{2, 9, "How do you do the freeze cancel?", "2026-02-20T14:00:00Z"},
		{3, 2, "Nice one! Keep grinding ", "2026-02-20T13:00:00Z"},
		{3, 6, "Gold league represent!", "2026-02-20T13:15:00Z"},
		{4, 7, "Clean setup! What monitor is that?", "2026-02-20T11:45:00Z"},
		{5, 10, "Shadow gameplay is always entertaining", "2026-02-20T11:00:00Z"},
		{8, 1, "Welcome to the arena! Good luck", "2026-02-19T21:00:00Z"},
		{8, 5, "We all started somewhere, you got this!", "2026-02-19T21:30:00Z"},
		{11, 4, "Rematch? I want that W back ", "2026-02-19T16:00:00Z"},
		{11, 1, "This match was legendary!", "2026-02-19T16:30:00Z"},
		{12, 2, "Welcome to Platinum! See you in Diamond soon", "2026-02-19T14:30:00Z"},
		{14, 3, "Your highlights are so motivating!", "2026-02-19T11:00:00Z"},
		{21, 8, "This guide helped me so much, thanks!", "2026-02-18T15:00:00Z"},
		{26, 3, "Inspiring journey! I'm working on the same goal", "2026-02-17T19:00:00Z"},
	}
	for _, c := range data {
		t, _ := time.Parse(time.RFC3339, c.createdAt)
		db.Exec(
			`INSERT INTO comments (post_id, author_id, text, created_at) VALUES ($1,$2,$3,$4)`,
			c.postID, c.authorID, c.text, t,
		)
	}
}

// ------------------------------------------------------------------------------
// Challenge CRUD
// ------------------------------------------------------------------------------

// CreateChallenge inserts a new challenge and optional visibility rows.
func CreateChallenge(payload CreateChallengePayload) (Challenge, error) {
	creatorID, err := strconv.Atoi(payload.CreatorID)
	if err != nil {
		return Challenge{}, fmt.Errorf("invalid creator ID")
	}

	var id int
	var createdAt time.Time
	err = db.QueryRow(
		`INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject, visibility)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at`,
		creatorID, payload.VideoURL, payload.ThumbnailURL, payload.Prefix, payload.Subject, payload.Visibility,
	).Scan(&id, &createdAt)
	if err != nil {
		return Challenge{}, err
	}

	// If friends visibility with specific users, insert visibility rows.
	if payload.Visibility == "friends" && len(payload.VisibleTo) > 0 {
		for _, uidStr := range payload.VisibleTo {
			uid, _ := strconv.Atoi(uidStr)
			if uid > 0 {
				db.Exec(`INSERT INTO challenge_visible_to (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, id, uid)
			}
		}
	}

	// Fetch the creator info.
	creator, _ := GetUserByID(payload.CreatorID)

	return Challenge{
		ID:              strconv.Itoa(id),
		CreatorID:       payload.CreatorID,
		CreatorUsername: creator.Username,
		CreatorLeague:   creator.League,
		VideoURL:        payload.VideoURL,
		ThumbnailURL:    payload.ThumbnailURL,
		Prefix:          payload.Prefix,
		Subject:         payload.Subject,
		Visibility:      payload.Visibility,
		Status:          "open",
		CreatedAt:       createdAt.UTC().Format(time.RFC3339),
	}, nil
}

// challengeBaseQuery is the common SELECT for challenges.
const challengeBaseQuery = `
SELECT c.id, c.creator_id, u.username, u.league,
	COALESCE(c.video_url, '') AS video_url,
	COALESCE(c.thumbnail_url, '') AS thumbnail_url,
	c.prefix, c.subject, c.visibility, c.status, c.views,
	COALESCE(lc.cnt, 0) AS likes,
	COALESCE(rc.cnt, 0) AS response_count,
	c.created_at
FROM challenges c
JOIN users u ON c.creator_id = u.id
LEFT JOIN (SELECT challenge_id, COUNT(*) AS cnt FROM challenge_likes GROUP BY challenge_id) lc ON lc.challenge_id = c.id
LEFT JOIN (SELECT challenge_id, COUNT(*) AS cnt FROM challenge_responses GROUP BY challenge_id) rc ON rc.challenge_id = c.id`

// queryChallenges executes a challenge query and returns Challenge structs.
func queryChallenges(query string, args ...interface{}) []Challenge {
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("queryChallenges error: %v", err)
		return nil
	}
	defer rows.Close()

	var result []Challenge
	for rows.Next() {
		var id, creatorID, views, likes, respCount int
		var username, league, videoURL, thumbURL, prefix, subject, visibility, status string
		var createdAt time.Time

		if rows.Scan(&id, &creatorID, &username, &league,
			&videoURL, &thumbURL,
			&prefix, &subject, &visibility, &status, &views,
			&likes, &respCount, &createdAt) == nil {

			result = append(result, Challenge{
				ID:              strconv.Itoa(id),
				CreatorID:       strconv.Itoa(creatorID),
				CreatorUsername: username,
				CreatorLeague:   league,
				VideoURL:        videoURL,
				ThumbnailURL:    thumbURL,
				Prefix:          prefix,
				Subject:         subject,
				Visibility:      visibility,
				Status:          status,
				Views:           views,
				Likes:           likes,
				ResponseCount:   respCount,
				CreatedAt:       createdAt.UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// GetArenaChallenges returns all challenges visible to everyone (arena).
func GetArenaChallenges() []Challenge {
	return queryChallenges(challengeBaseQuery + `WHERE c.visibility ='arena' ORDER BY c.created_at DESC`)
}

// GetFriendsChallenges returns challenges visible to a specific user (friends-only).
// This includes: challenges by people the user follows with visibility=friends,
// where the user is either in the visible_to list OR the list is empty (all friends).
func GetFriendsChallenges(userID string) []Challenge {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return nil
	}
	query := challengeBaseQuery + `
	WHERE c.visibility = 'friends' 
	  AND c.creator_id IN (SELECT following_id FROM follows WHERE follower_id =$1)
	  AND (
		NOT EXISTS (SELECT 1 FROM challenge_visible_to WHERE challenge_id = c.id)
		OR c.id IN (SELECT challenge_id FROM challenge_visible_to WHERE user_id = $1)
	  )
	ORDER BY c.created_at DESC`
	return queryChallenges(query, uid)
}

// GetChallengeByIDreturns a single challenge.
func GetChallengeByID(idStr string) (Challenge, bool) {
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return Challenge{}, false
	}
	results := queryChallenges(challengeBaseQuery+` WHERE c.id = $1`, id)
	if len(results) == 0 {
		return Challenge{}, false
	}
	return results[0], true
}

// GetChallengeResponses returns all responses to a challenge.
func GetChallengeResponses(challengeID string) []ChallengeResponse {
	cid, err := strconv.Atoi(challengeID)
	if err != nil {
		return nil
	}

	rows, err := db.Query(
		`SELECT cr.id, cr.challenge_id, cr.responder_id, u.username, u.league,
				COALESCE(cr.video_url, '') AS video_url,
				COALESCE(cr.thumbnail_url, '') AS thumbnail_url,
				cr.views,
				COALESCE(lc.cnt, 0) AS likes,
				cr.created_at
		 FROM challenge_responses cr
		 JOIN users u ON cr.responder_id = u.id
		 LEFT JOIN (SELECT response_id, COUNT(*) AS cnt FROM challenge_response_likes GROUP BY response_id) lc ON lc.response_id = cr.id
		 WHERE cr.challenge_id = $1
		 ORDER BY cr.created_at ASC`, cid,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []ChallengeResponse
	for rows.Next() {
		var id, chalID, respID, views, likes int
		var username, league, videoURL, thumbURL string
		var createdAt time.Time
		if rows.Scan(&id, &chalID, &respID, &username, &league,
			&videoURL, &thumbURL, &views, &likes, &createdAt) == nil {
			result = append(result, ChallengeResponse{
				ID:                strconv.Itoa(id),
				ChallengeID:       strconv.Itoa(chalID),
				ResponderID:       strconv.Itoa(respID),
				ResponderUsername: username,
				ResponderLeague:   league,
				VideoURL:          videoURL,
				ThumbnailURL:      thumbURL,
				Views:             views,
				Likes:             likes,
				CreatedAt:         createdAt.UTC().Format(time.RFC3339),
			})
		}
	}
	return result
}

// AcceptChallenge inserts a response and updates challenge status.
func AcceptChallenge(payload AcceptChallengePayload) (ChallengeResponse, error) {
	cid, err1 := strconv.Atoi(payload.ChallengeID)
	rid, err2 := strconv.Atoi(payload.ResponderID)
	if err1 != nil || err2 != nil {
		return ChallengeResponse{}, fmt.Errorf("'invalid IDs")
	}

	var id int
	var createdAt time.Time
	err := db.QueryRow(
		`INSERT INTOchallenge_responses (challenge_id, responder_id, video_url, thumbnail_url)
		 VALUES ($1,$2,$3,$4) RETURNING id, created_at`,
		cid, rid, payload.VideoURL, payload.ThumbnailURL,
	).Scan(&id, &createdAt)
	if err != nil {
		return ChallengeResponse{}, err
	}

	// Update challenge status to "active".
	db.Exec(`UPDATE challenges SET status = 'active' WHERE id = $1 AND status = 'open'`, cid)

	responder, _ := GetUserByID(payload.ResponderID)

	return ChallengeResponse{
		ID:                strconv.Itoa(id),
		ChallengeID:       payload.ChallengeID,
		ResponderID:       payload.ResponderID,
		ResponderUsername: responder.Username,
		ResponderLeague:   responder.League,
		VideoURL:          payload.VideoURL,
		ThumbnailURL:      payload.ThumbnailURL,
		CreatedAt:         createdAt.UTC().Format(time.RFC3339),
	}, nil
}

// ToggleChallengeLike toggles a like on a challenge.
func ToggleChallengeLike(challengeID, userID string) (bool, int) {
	cid, e1 := strconv.Atoi(challengeID)
	uid, e2 := strconv.Atoi(userID)
	if e1 != nil || e2 != nil {
		return false, 0
	}

	var exists bool
	db.QueryRow(`SELECT EXISTS(SELECT 1 FROM challenge_likes WHERE challenge_id=$1 AND user_id=$2)`, cid, uid).Scan(&exists)

	if exists {
		db.Exec(`DELETE FROM challenge_likes WHERE challenge_id=$1 AND user_id=$2`, cid, uid)
	} else {
		db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, cid, uid)
	}

	var count int
	db.QueryRow(`SELECT COUNT(*) FROM challenge_likes WHERE challenge_id=$1`, cid).Scan(&count)
	return !exists, count
}

// IncrementChallengeViews bumps the view count for a challenge.
func IncrementChallengeViews(challengeID string) {
	cid, err := strconv.Atoi(challengeID)
	if err == nil {
		db.Exec(`UPDATE challenges SET views = views + 1 WHERE id = $1`, cid)
	}
}

// --------------------------------------------------------------------------------
// Seed challenges (sample data)
// --------------------------------------------------------------------------------

func seedChallenges() {
	type sc struct {
		creatorID          int
		videoURL, thumbURL string
		prefix, subject    string
		visibility, status string
		views              int
		createdAt          string
	}
	data := []sc{
		{2, "https://cdn.pixabay.com/video/2026/02/09/333600_small.mp4", "https://cdn.pixabay.com/video/2026/02/09/333600_small.jpg", "Who is better", "Gamer", "arena", "open", 3200, "2026-03-10T14:00:00Z"},
		{7, "", "", "Which is best", "Combo Move", "arena", "open", 1800, "2026-03-10T12:00:00Z"},
		{1, "https://cdn.pixabay.com/video/2026/02/15/334716_small.mp4", "https://cdn.pixabay.com/video/2026/02/15/334716 small.jpg", "Who can beat", "This Record", "arena", "open", 950, "2026-03-09T20:00:00Z"},
		{4, "", "", "Who has better", "Strategy", "arena", "active", 4500, "2026-03-09T16:00:00Z"},
		{10, "", "", "Who is the real", "Champion", "arena", "open", 2100, "2026-03-08T10:00:00Z"},
		{6, "", "", "Which is best", "Setup", "friends", "open", 600, "2026-03-10T08:00:00Z"},
		{2, "https://cdn.pixabay.com/video/2026/01/09/326739_small.mp4", "https://cdn.pixabay.com/video/2026/01/09/326739_small.jpg", "Who is better", "Sniper", "friends", "open", 400, "2026-03-09T18:00:00Z"},
		{5, "", "", "Who can do better", "Trick Shot", "arena", "open", 1500, "2026-03-08T22:00:00Z"},
	}
	for _, c := range data {
		t, _ := time.Parse(time.RFC3339, c.createdAt)
		db.Exec(
			`INSERT INTO challenges (creator_id, video_url, thumbnail_url, prefix, subject, visibility, status, views, created_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			c.creatorID, c.videoURL, c.thumbURL, c.prefix, c.subject, c.visibility, c.status, c.views, t,
		)
	}

	// Seed some challenge likes.
	challengeLikes := [][2]int{{1, 1}, {1, 3}, {1, 5}, {1, 7}, {2, 2}, {2, 4}, {3, 1}, {3, 2}, {4, 1}, {4, 3}, {4, 6}, {4, 7}, {4, 9}, {5, 2}, {5, 7}, {8, 1}, {8, 4}}
	for _, cl := range challengeLikes {
		db.Exec(`INSERT INTO challenge_likes (challenge_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, cl[0], cl[1])
	}

	// Seed a response for challenge 4 (status = active).
	t, _ := time.Parse(time.RFC3339, "2026-03-09T17:30:00Z")
	db.Exec(
		`INSERT INTO challenge_responses (challenge_id, responder_id, video_url, thumbnail_url, views, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		4, 2, "https://cdn.pixabay.com/video/2026/02/15/334716_small.mp4", "https://cdn.pixabay.com/video/2026/02/15/334716_small.jpg", 3100, t,
	)
}
