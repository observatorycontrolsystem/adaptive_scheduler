class StressTestNightParams:
    def __init__(self):
        self.numresources=2
        self.numdays=6*30 # 6 months
	self.tfinal = self.numdays*24*60
        self.numreservations = 200
        self.night_length = 12*60 
        self.priority_range=[1,1] 
        self.duration_range=[1*60,1*60] 
        self.slack_range=[0,0] 
        self.oneof_elt_num_range=[0,0]
        self.and_elt_num_range=[0,0]
        self.slice_length = 10 # 10 min
        self.outputfname = 'output_pn1.txt'
