package model

import "encoding/binary"

type ActorID [16]byte

type ActorRef struct{ ID ActorID }

const (
	actorIDTimestampBits   = 48
	actorIDRuntimeBits     = 16
	actorIDIncarnationBits = 16
	actorIDSequenceBits    = 48

	actorIDTimestampMax   uint64 = 1<<actorIDTimestampBits - 1
	actorIDRuntimeMax     uint16 = 1<<actorIDRuntimeBits - 1
	actorIDIncarnationMax uint16 = 1<<actorIDIncarnationBits - 1
	actorIDSequenceMax    uint64 = 1<<actorIDSequenceBits - 1
)

func NewActorID(timestampMS uint64, runtimeSlot uint16, incarnation uint16, sequence uint64) (ActorID, error) {
	var id ActorID
	if timestampMS > actorIDTimestampMax {
		return id, ErrActorIDTimestampOverflow
	}
	if sequence > actorIDSequenceMax {
		return id, ErrActorIDSequenceOverflow
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], timestampMS)
	copy(id[0:6], buf[2:8])
	binary.BigEndian.PutUint16(id[6:8], runtimeSlot)
	binary.BigEndian.PutUint16(id[8:10], incarnation)
	binary.BigEndian.PutUint64(buf[:], sequence)
	copy(id[10:16], buf[2:8])
	return id, nil
}

func (id ActorID) TimestampMS() uint64 {
	var buf [8]byte
	copy(buf[2:8], id[0:6])
	return binary.BigEndian.Uint64(buf[:])
}

func (id ActorID) RuntimeSlot() uint16 {
	return binary.BigEndian.Uint16(id[6:8])
}

func (id ActorID) Incarnation() uint16 {
	return binary.BigEndian.Uint16(id[8:10])
}

func (id ActorID) Sequence() uint64 {
	var buf [8]byte
	copy(buf[2:8], id[10:16])
	return binary.BigEndian.Uint64(buf[:])
}

func (id ActorID) Split() (timestampMS uint64, runtimeSlot uint16, incarnation uint16, sequence uint64) {
	return id.TimestampMS(), id.RuntimeSlot(), id.Incarnation(), id.Sequence()
}
