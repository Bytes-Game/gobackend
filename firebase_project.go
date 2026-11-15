package main

// firebaseProjectNote is what the server adds to its start-up line about
// notifications: the Firebase project it sends them through.
//
// The app is built for one Firebase project, and the server holds the
// private key of one. They must be the same project. A phone registered
// with one project cannot be reached through another: every send fails
// (Firebase answers SENDER_ID_MISMATCH), the phone's address is kept and
// tried again, and nothing anywhere says why no notification arrives. On
// 15 November the app moved to a new project (battle-adddb) while the
// server still held the old project's key. So the line names the project,
// for the owner to compare with the app's.
//
// Empty when notifications do not go through Firebase, or when its key is
// missing (that has its own line).
func firebaseProjectNote(s PushSender) string {
	var f *fcmSender
	switch v := s.(type) {
	case *fcmSender:
		f = v
	case *multiSender:
		f, _ = v.byPlatform["fcm"].(*fcmSender)
	}
	if f == nil || !f.configured() {
		return ""
	}
	return ", Firebase project " + f.projectID +
		". The app must be built for the same project, or no notification reaches a phone."
}
