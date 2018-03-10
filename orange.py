class RebalanceObject(object):
    def __init__(self, first, next):
        self.first = first
        self.next = next

class Chunk(object):
    def __init__(self):
        self.min_key = None
        self.keys = []
        self.values = []
        self.key_count = 0
        self.val_count = 0
        self.batched_index = 0 #Voodoo
        self.next_chunk = None
        self.is_next_marked = False
        self.rebalance_data = RebalanceObject() #Voodoo

    def find_latest(self, key):
        #this is shit
        for index, chunk_key in enumerate(self.keys):
            if chunk_key == key:
                return self.values[index]

    def add_to_list(self, allocated_key):
        pass

    def rebalance(self, put_key, put_val):
        tmp = RebalanceObject(self, self.next_chunk)
        CAS(self.rebalance_data.ro, None, tmp)
        ro = self.rebalance_data.ro
        last = self
        while ro.next != None:
            next = ro.next
            if policy(next):
                CAS(next.ro, None, ro)
                if next.ro:
                    CAS(ro.next, next, next.next)
                    last = next
                else:
                    CAS(ro.next, next, None)
            else:
                CAS(ro.next, next, None)

        while last.next.ro = ro:
            last = last.next

        for c from ro.first to last:
            c.status = frozen

        c_f = c_n = new_chunk
        min_key = self.min_key
        parent = self
        status = infant

        for c_0 from ro.first to last:
            if c_0.min_key <= put_key < c_0.next.min_key:
                to_put = [(put_key, put_val)]
            else:
                to_put = []
            for k in sorted(c_0.k union to_put):
                if len(c_n) > c_n.size / 2:
                    c_n.next = Chunk(min_key=k, parent=self, status=infant)
                    c_n = c_n.next
                #todo get val from k
                if val == None:
                    break
                c_n.insert(k, val)
        do:
            c_n.next = last.next
        while not CAS(last.next+mark, c_n.next+false, c_n.next+true)

        do:
            pred = self.pred
            if CAS(pred.next+mark, self+false, c_f + false):
                normalize(self)
                return true
            if pred.next.parent = self:
                normalize(self)
                return false
            return rebalance(pred, None, None)
        while true

def locate_chunk_by_key(key):
    pass


def help_pending_puts(chunk, from_key, to_key):
    pass #might need this shit

def get(key):
    chunk = locate_chunk_by_key(key)
    help_pending_puts()
    return chunk.find(key)

def check_rebalance(chunk, key, val):
    pass

def rebalance(chunk, key, val):
    return True

def put(key, val):
    chunk = locate_chunk_by_key(key)
    if check_rebalance(chunk, key, val):
        return
    val_index = fetch_and_add(chunk.val_count, val.size)
    key_index = fetch_and_inc(chunk.key_count)
    if val_index > len(chunk.values) or key_index > len(chunk.keys):
        if not rebalance(chunk, key, val):
            put(key, val) #fix fucking recursion
    chunk.values[val_index] = val
    chunk.keys[key_index] = (key, val_index, None)
    do:
        c = find(key, chunk)
        if c is None:
            if chunk.add_to_list(key_index):
                return
        else:
            val_index_tag = c.val_ptr
            if val_index_tag < val_index:
                CAS(c.val_ptr, val_index_tag, val_index)
    while c.val_ptr < val_index

    while chunk.val_ptr >= val_index

def normalize(chunk):
    #TODO: understand this crap
    for c from C.ro.first to c.ro.last:
        index.