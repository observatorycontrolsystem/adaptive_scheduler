class IComparator(object):
    def compare(self):
        pass
        

class SimplePriorityComparator(IComparator):
    def compare(self, slot1, slot2):
        return (slot1.priority < slot2.priority)


class AlwaysTrueComparator(IComparator):
    def compare(self, slot1, slot2):
        return True
