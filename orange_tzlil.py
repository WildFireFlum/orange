class RebalanceObject(object):
	"""doc string for RebalanceObject"""
	def __init__(self, first, next):
		super(RebalanceObject, self).__init__()
		self.first = first
		self.next = next

def policy(fuck):
	pass

class Chunk(object):
	"""doc string for Chunk"""
	def __init__(self, min_key, parent, status):
		super(Chunk, self).__init__()
		self.min_key = min_key
		self.k = [] # in-chunk - linked list
		self.v = [] # values
		self.k_counter = 0
		self.v_counter = 0
		self.batched_index = 0 # TODO what the fuck is that??
		self.next = None # next chunk
		self.mark = False # indicates whether next is immutable
		self.rebalanced_data = (parent, status, None) # rebalancing related data

	def find(self, key):
		pass # TODO: binary search

	def check_rebalance(self, key, val):
		pass

	def rebalanced(chunk, key, val):
		# 1. engage

		# TODO: read policy and understand which chunks are in...
		CAS(chunk.rebalanced_data.ro, None, RebalanceObject(first=chunk, next=chunk.next))
		ro = chunk.rebalanced_data.ro
		last = chunk
		while ro.next is not None:
			next = ro.next
			if policy(next):
				CAS(ro.next, next, next.next)
				last = next
			else
				CAS(ro.next, next, None)
		# search for the last concurrently engaged chunk 
		while last.next.ro == ro:
			last = last.next

		# freeze
		for c in [ro.first, ... ro.last]:
			c.status = frozen

		# build
		cf = cn = Chunk(min_key=chunk.min_key, parent=chunk, status=infant)
		for c0 in [ro.first, ... ro.last]:
			if c0.min_key <= key < c0.next.min_key:
				to_put = [(key, val)]
			else:
				to_put = []
			for k in sorted(c0.k + to_put):
				if len(cn) > cn.size / 2: # cn is more then half full
					cn.next = Chunk(min_key=k, parent=chunk, status=infant)
					cn = cn.next
				cn.insert(k, val) # TODO: get val from k ???

		# replace
		do
			cn.next = last.next
		while not CAS(last.next + mark, cn.next + false, cn.next + true)
		do
			pred = c.predecessor()
			if CAS(pred.next + mark, c + false, cf + false)
				normalize(chunk)
				return false
			return rebalanced(pred, None, None) # fuck my life, what is going on here...
		while true 

		def normalize():
			pass # fuck me :O

	def add_i_to_list(i):
		pass # use CAS

def locate_target_chunk(key):
	pass # TODO

def help_pending_puts(chunk, from_key, to_key):
	pass

def get(key):
	chunk = locate_target_chunk(key)
	help_pending_puts(chunk, key, key)
	return chunk.find(key)

def put(key, val):
	chunk = locate_target_chunk(key)
	if chunk.check_rebalance(key, val):
		return # required rebalance completed the put
	j = fetch_and_add(chunk.v_counter, val.size)
	i = fetch_and_increment(chunk.k_counter)

	if j >= c.v.size or i >= c.k.size:
		if not chunk.rebalance(key, val):
			return put(key, val)

	chunk.v[j] = val
	chunk.k[i] = (key, j,  None) # None for next - not seted yet

	do:
		c = chunk.find(key)
		if c is None:
			if chunk.add_i_to_list(i):
				break
		else:
			j_tag = c.val_ptr
			if j_tag < j:
				CAS(c.val_ptr, j_tag, j) 
	while c.val_ptr < j