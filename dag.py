"""Main module."""
from datetime import datetime, timedelta
from dj_schemas.experiment import fids, FileID
from dj_schemas.lfp import EventRelatedPotential
from dj_schemas.power_and_coherence import PowerWavelet
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

import datajoint as dj
import os
os.environ["GOTO_NUM_THREADS"] = "1"  # force OpenBLAS to use one thread

dj.config['database.host'] = '127.0.0.1'
dj.config['database.user'] = 'root'
dj.config['database.password'] = 'jjRQrzX6p9sHX?f9xb27SsbJWbYCsAACk!Em_gRc2D*J=a?'

if os.getenv('dj_schema_name') is None:
    # Default schema if no environment variable
    schema = dj.schema('mmyros_chirp014', create_tables=True)
    #schema = dj.schema('mmyros_tcloop', create_tables=True)
else:
    # Get schema name from env var eg
    # os.environ['dj_schema_name']= 'model_ih' or import os;os.environ['dj_schema_name']= 'mmyros_chirp014'
    schema = dj.schema(os.getenv('dj_schema_name'), create_tables=True)
schema.spawn_missing_classes()


with DAG(dag_id='airjoint',
         default_args={
             'depends_on_past': False,
             'retries': 1,
             'retry_delay': timedelta(minutes=5),
         },
         description='DAG to populate datajoint schemas',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2021, 1, 1),
         catchup=False,
         tags=['example'],
         ) as dag:

    
    @task(task_id='fids')
    def get_fids():
        fids()
        print(FileID())

    def djt(schema,
                suppress_errors=False,
                display_progress=False,
                reserve_jobs=False,
                order='random',
                max_calls=None):
        @task(task_id=schema.table_name[2:])
        def schema_task():
            print(schema())
            number_of_entries_before = len(schema())
            #schema.populate(suppress_errors=suppress_errors,
            #                display_progress=display_progress,
            #                reserve_jobs=reserve_jobs,
            #                order=order)
            number_of_entries_after = len(schema())
            return number_of_entries_after - number_of_entries_before
        return schema_task()

    
    meta = [djt(this_task) for this_task in [AboutSubject, RecordingSessionParameters, ExtraMetaData, MazeTrials,
                                             SessionManipulationType, SessionManipulationTypes, SessionStimType, MazeParameters, ExtraEvents,
                                             StimExtraParams]]
    calculate = [djt(this_task) for this_task in
                 [EventRelatedPotential]*8 + [PowerWavelet]*3 +[ConnectivityX]*3 +
                 [HistogramOfSignal, LfpQuality,ConnectivityX, Power, HistogramOfSignalByPhase]]
    summarize = [djt(this_task) for this_task in
                 [PowerMeanOfTime, PowerAt, PowerMeanOfTimeWavelet, PowerAtWavelet, ConnectivityAtStimFrequencyX,
                  PowerWaveletMeanBand, PowerMeanBand, ConnectivityMeanBand, ConnectivityMax, PowerMax, PhaseGoodness]
                 *2]
    spikesort = [djt(this_task) for this_task in
                 [Probe, SpikesortingConversion, Spikesorting]]
    curate = [djt(this_task) for this_task in
              [SpikesPreCuration, SpikesortingQuality, SpikesCurated,Spikes, SpikesortingCurationQuality]]
    spikes = [djt(this_task) for this_task in
              [SpikePhase, EventRelatedSpikesAndPhases, SpikeStats, PhaseLocking]]
    phase = [djt(this_task) for this_task in
             [LfpPhase, LfpPhaseXarrayStore, EventRelatedLfpPhase, PhaseAmplitudeCoupling, CrossFrequencyCoupling]]

    erp = djt(EventRelatedPotential, max_calls=1000)
    get_fids() >>  djt(BlackrockComments)>> [djt(Events), djt(LfpFromBlackrock)]>> meta >> erp  >> calculate >> erp >> summarize >> djt(Probe) 
