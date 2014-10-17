class StressTestParams:
    def __init__(self):
        self.numresources=1
        self.tfinal=30*24*60*60 # 1 month
        self.numreservations = 30*10 # avg 10 per day
        self.priority_range=[1,100] # random
        self.duration_range=[1,8*60*60] # max 8 hrs
        self.slack_range=[0,60*60] # max 1 hr
        self.oneof_elt_num_range=[0,0]
        self.and_elt_num_range=[0,0]
        self.slice_length = 10*60 # 10 min
        self.outputfname = 'output_p2.txt'
        
