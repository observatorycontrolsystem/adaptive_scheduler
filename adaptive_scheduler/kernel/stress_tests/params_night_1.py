class StressTestNightParams:
    def __init__(self):
        self.numresources=12
        self.numdays=6*30 # 6 months
	self.tfinal = self.numdays*24*60
        self.avg_res_per_night_per_resource = 5
        self.numreservations = self.avg_res_per_night_per_resource * self.numresources * self.numdays
        self.night_length = 12*60 # 12 hrs in min
        self.priority_range=[1,100] 
        self.duration_range=[1,8*60] # max 8 hrs
        self.slack_range=[0,4*60] # max 4 hr
        self.oneof_elt_num_range=[2,3]
        self.and_elt_num_range=[2,3]
        self.slice_length = 10 # 10 min
        self.outputfname = 'output_pn1.txt'
