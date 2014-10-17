class StressTestParams:
    def __init__(self):
        self.numresources=1
        self.tfinal=7*24*60 # 1 week in minutes
        self.numreservations = 7*10 # avg 10 per day
        self.priority_range=[1,1] # flat
        self.duration_range=[1,8*60] # max 8 hrs
        self.slack_range=[0,60] # max 1 hr
        self.oneof_elt_num_range=[0,0]
        self.and_elt_num_range=[0,0]
        self.slice_length = 0
        self.outputfname = 'output_p1.txt'
        
