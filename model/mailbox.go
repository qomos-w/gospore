package model

type MailboxType int

const (
	UnboundedMailbox MailboxType = iota
	BoundedMailbox
)
