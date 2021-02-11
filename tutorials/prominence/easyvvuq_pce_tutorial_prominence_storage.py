import os
import time
import easyvvuq as uq
import chaospy as cp

# Set up a fresh campaign called "coffee_pce"
my_campaign = uq.Campaign(name='coffee_pce')

# Define parameter space
params = {
    "temp_init": {"type": "float", "min": 0.0, "max": 100.0, "default": 95.0},
    "kappa": {"type": "float", "min": 0.0, "max": 0.1, "default": 0.025},
    "t_env": {"type": "float", "min": 0.0, "max": 40.0, "default": 15.0},
    "out_file": {"type": "string", "default": "output.csv"}
}

# Create encoder and decoder
encoder = uq.encoders.GenericEncoder(
    template_fname='cooling.template',
    delimiter='$',
    target_filename='cooling_in.json')

decoder = uq.decoders.SimpleCSV(target_filename="output.csv", output_columns=["te"])

# Add the app (automatically set as current app)
my_campaign.add_app(name="cooling",
                    params=params,
                    encoder=encoder,
                    decoder=decoder)

# Create the sampler
vary = {
    "kappa": cp.Uniform(0.025, 0.075),
    "t_env": cp.Uniform(15, 25)
}
my_sampler = uq.sampling.PCESampler(vary=vary, polynomial_order=3)

# Associate the sampler with the campaign
my_campaign.set_sampler(my_sampler)

# Will draw all (of the finite set of samples)
my_campaign.draw_samples()

my_campaign.populate_runs_dir()

# We don't specify any input or output files here because we're using shared storage
statuses = my_campaign.apply_for_each_run_dir(uq.actions.ExecuteProminence("coffee-b2drop.json"), batch_size=3)
statuses.start()

# Wait for all jobs to finish
while (statuses.progress()['ready'] + statuses.progress()['active'] > 0):
    time.sleep(2)

print('Finished running jobs, status:', statuses.progress())

my_campaign.collate()

# Post-processing analysis
my_analysis = uq.analysis.PCEAnalysis(sampler=my_sampler, qoi_cols=["te"])
my_campaign.apply_analysis(my_analysis)

# Get some descriptive statistics
results = my_campaign.get_last_analysis()
print(results.describe())
