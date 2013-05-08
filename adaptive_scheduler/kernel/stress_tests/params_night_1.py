class StressTestNightParams:
    def __init__(self):
        self.numresources=1
        self.numdays=6*30 # 6 months
        self.avg_res_per_night_per_resource = 10
        self.numreservations = self.avg_res_per_night_per_resource * self.numresources * self.numdays
        self.night_length = 8*60*60 # 8 hrs
        self.priority_range=[0,0] # flat
        self.duration_range=[1,8*60*60] # max 8 hrs
        self.slack_range=[1,60*60] # max 1 hr
        self.oneof_elt_num_range=[0,0]
        self.and_elt_num_range=[0,0]
        self.slice_length = 10*60 # 10 min

