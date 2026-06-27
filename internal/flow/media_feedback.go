package flow

func feedbackFrameStats(fs FrameStats) (frames, decFrames, keys, decKeys uint32) {
	return satUint32(fs.Frames), satUint32(fs.DecodableFrames),
		satUint32(fs.Keyframes), satUint32(fs.DecodableKeyframes)
}

func satUint32(v uint64) uint32 {
	if v > uint64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(v)
}
