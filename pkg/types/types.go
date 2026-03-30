// Package types defines shared types used across bridge and matrix packages.
package types

import (
	"context"

	"maunium.net/go/mautrix/id"
)

// RoomInfo describes a voice room.
type RoomInfo struct {
	Name   string
	RoomID id.RoomID
}

// SlotInfo describes a sidecar slot.
type SlotInfo struct {
	Index       int
	Status      string // "free", "busy", "dead"
	ChannelID   uint64
	ChannelName string
	Token       string // masked
}

// ManagerAPI is the interface for admin command interaction with the bridge manager.
type ManagerAPI interface {
	Stats() (activeBridges, busySlots, totalSlots, trackedUsers int)
	ListRooms() map[uint64]RoomInfo
	ListSlots() []SlotInfo
	ManualJoin(ctx context.Context, channelName string) error
	ManualLeave(ctx context.Context, channelName string) error
}
