package gmgo

import "testing"

func TestTailing(t *testing.T) {
	session := testDBSession()
	StartTailing(session)
}
