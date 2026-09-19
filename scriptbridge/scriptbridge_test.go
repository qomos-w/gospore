package scriptbridge

import (
	"strings"
	"testing"
)

func TestAnnotationConstants(t *testing.T) {
	cases := []struct{ got, want string }{
		{AnnotationExecution, "gospore.execution"},
		{AnnotationNamespace, "gospore.namespace"},
		{AnnotationFrameKind, "gospore.frame_kind"},
		{AnnotationVisibility, "gospore.visibility"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
}

func TestStaticMethodConstants(t *testing.T) {
	cases := []struct{ got, want string }{
		{StaticProjectionGet, "gospore.projection.get"},
		{StaticProjectionField, "gospore.projection.field"},
		{StaticProjectionWatch, "gospore.projection.watch"},
		{StaticEventsSubscribeService, "gospore.events.subscribe_service"},
		{StaticEventsSubscribeInstance, "gospore.events.subscribe_instance"},
		{StaticEventsRecent, "gospore.events.recent"},
		{StaticAppLookupPath, "gospore.app.lookup_path"},
		{StaticAppLookupService, "gospore.app.lookup_service"},
		{StaticAppTree, "gospore.app.tree"},
		{StaticPolicyCheck, "gospore.policy.check"},
		{StaticPolicyVersion, "gospore.policy.version"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
}

func TestPrefixInvoke(t *testing.T) {
	if PrefixInvoke != "gospore.invoke." {
		t.Errorf("PrefixInvoke = %q", PrefixInvoke)
	}
	if !strings.HasPrefix("gospore.invoke.auth.login", PrefixInvoke) {
		t.Error("PrefixInvoke should match dynamic invoke methods")
	}
}
