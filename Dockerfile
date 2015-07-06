FROM tutum.co/teltech/scratch-teltech

	ADD dist/platform /platform

	

	ENTRYPOINT ["/platform"]
