
dirs:=proxy proxy/rawkvproxy ttltask gc

all:
	@for dir in ${dirs}; do \
		make -C $$dir; \
	done

.PHONY: clean
	
clean:
	@for dir in ${dirs}; do \
		make -C $$dir clean; \
	done
