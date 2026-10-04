package http

import (
	nethttp "net/http"
)

// eraseContact removes what RelayPlane keeps about a person for the caller's tenant (LGPD/GDPR erasure). The number is
// in the path; the answer reports counts and never repeats it.
func (s *Server) eraseContact(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) {
	rep, err := s.App.Contacts.Erase(r.Context(), p.TenantID, r.PathValue("number"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, rep)
}
