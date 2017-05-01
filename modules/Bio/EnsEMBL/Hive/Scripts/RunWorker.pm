=head1 LICENSE

Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
Copyright [2016-2017] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut


package Bio::EnsEMBL::Hive::Scripts::RunWorker;

use strict;
use warnings;

use Bio::EnsEMBL::Hive::Valley;
use Bio::EnsEMBL::Hive::Utils::Stopwatch;


sub add_connection_command_line_options { 1 };

sub command_line_options {
    return [

        # Task specification parameters:
        'rc_id=i'                    => 'resource_class_id',
        'rc_name=s'                  => 'resource_class_name',
        'analyses_pattern=s'         => 'analyses_pattern',
        'analysis_id=i'              => 'analysis_id',
        'logic_name=s'               => 'logic_name',
        'job_id=i'                   => 'job_id',
        'force=i'                    => 'force',
        'beekeeper_id=i'             => 'beekeeper_id',

        # Worker control parameters:
        'job_limit=i'                => 'job_limit',
        'life_span|lifespan=i'       => 'life_span',
        'no_cleanup'                 => 'no_cleanup',
        'no_write'                   => 'no_write',
        'hive_log_dir|hive_output_dir=s'         => 'hive_log_dir',       # keep compatibility with the old name
        'worker_log_dir|worker_output_dir=s'     => 'worker_log_dir',     # will take precedence over hive_log_dir if set
        'retry_throwing_jobs=i'      => 'retry_throwing_jobs',
        'can_respecialize=i'         => 'can_respecialize',

        # Other commands
        'debug=i'                    => 'debug',
    ];
}


sub run {
    my $self = shift;

    my $analyses_pattern = $self->param('analyses_pattern');
    if( $self->param('logic_name') ) {
        die "-logic_name cannot be set at the same time as -analyses_pattern.\n" if $analyses_pattern;
        warn "-logic_name is now deprecated, please use -analyses_pattern that extends the functionality of -logic_name and -analysis_id .\n";
        $analyses_pattern = $self->param('logic_name');
    } elsif ( $self->param('analysis_id') ) {
        die "-analysis_id cannot be set at the same time as -analyses_pattern.\n" if $analyses_pattern;
        warn "-analysis_id is now deprecated, please use -analyses_pattern that extends the functionality of -analysis_id and -logic_name .\n";
        $analyses_pattern = $self->param('analysis_id');
    }

    my %specialization_options = (
        resource_class_id   => $self->param('resource_class_id'),
        resource_class_name => $self->param('resource_class_name'),
        can_respecialize    => $self->param('can_respecialize'),
        analyses_pattern    => $analyses_pattern,
        job_id              => $self->param('job_id'),
        force               => $self->param('force'),
        beekeeper_id        => $self->param('beekeeper_id'),
    );
    my %life_options = (
        job_limit           => $self->param('job_limit'),
        life_span           => $self->param('life_span'),
        retry_throwing_jobs => $self->param('retry_throwing_jobs'),
    );
    my %execution_options = (
        no_cleanup          => $self->param('no_cleanup'),
        no_write            => $self->param('no_write'),
        worker_log_dir      => $self->param('worker_log_dir'),
        hive_log_dir        => $self->param('hive_log_dir'),
        debug               => $self->param('debug'),
    );

    runWorker($self->param('pipeline'), \%specialization_options, \%life_options, \%execution_options);
}

sub runWorker {
    my ($pipeline, $specialization_options, $life_options, $execution_options) = @_;

    my $worker_stopwatch = Bio::EnsEMBL::Hive::Utils::Stopwatch->new();
    $worker_stopwatch->_unit(1); # lifespan_sec is in seconds
    $worker_stopwatch->restart();

    my $hive_dba = $pipeline->hive_dba;

    die "Hive's DBAdaptor is not a defined Bio::EnsEMBL::Hive::DBSQL::DBAdaptor\n" unless $hive_dba and $hive_dba->isa('Bio::EnsEMBL::Hive::DBSQL::DBAdaptor');

    $specialization_options ||= {};
    $life_options ||= {};
    $execution_options ||= {};

    my $queen = $hive_dba->get_Queen();
    die "No Queen, God Bless Her\n" unless $queen and $queen->isa('Bio::EnsEMBL::Hive::Queen');

    if( $specialization_options->{'force_sync'} ) {       # sync the Hive in Test mode:
        my $list_of_analyses = $pipeline->collection_of('Analysis')->find_all_by_pattern( $specialization_options->{'analyses_pattern'} );

        $queen->synchronize_hive( $list_of_analyses );
    }

    # Create the worker
    my $worker = $queen->create_new_worker(
          # Resource class:
             -resource_class_id     => $specialization_options->{'resource_class_id'},
             -resource_class_name   => $specialization_options->{'resource_class_name'},
             -beekeeper_id          => $specialization_options->{'beekeeper_id'},

          # Worker control parameters:
             -job_limit             => $life_options->{'job_limit'},
             -life_span             => $life_options->{'life_span'},
             -no_cleanup            => $execution_options->{'no_cleanup'},
             -no_write              => $execution_options->{'no_write'},
             -worker_log_dir        => $execution_options->{'worker_log_dir'},
             -hive_log_dir          => $execution_options->{'hive_log_dir'},
             -retry_throwing_jobs   => $life_options->{'retry_throwing_jobs'},
             -can_respecialize      => $specialization_options->{'can_respecialize'},

          # Other parameters:
             -debug                 => $execution_options->{'debug'},
    );
    die "No worker !\n" unless $worker and $worker->isa('Bio::EnsEMBL::Hive::Worker');

    # Run the worker
    eval {
        $worker->run( {
             -analyses_pattern      => $specialization_options->{'analyses_pattern'},
             -job_id                => $specialization_options->{'job_id'},
             -force                 => $specialization_options->{'force'},
        } );
        cleanup_if_needed($worker);
        _update_resource_usage($worker, $worker_stopwatch);
        $hive_dba->dbc->disconnect_if_idle;
        1;

    } or do {
        my $msg = $@;
        eval {
            $hive_dba->get_LogMessageAdaptor()->store_worker_message($worker, $msg, 'WORKER_ERROR' );
            $worker->cause_of_death( 'SEE_MSG' );
            $queen->register_worker_death($worker, 1);
        };
        $msg .= "\nAND THEN:\n".$@ if $@;
        cleanup_if_needed($worker);
        _update_resource_usage($worker, $worker_stopwatch, 'error');

        $hive_dba->dbc->disconnect_if_idle;
        die $msg;
    };

}

        # have runnable clean up any global/process files/data it may have created
sub cleanup_if_needed {
    my ($worker) = @_;
    if($worker->perform_cleanup) {
        if(my $runnable_object = $worker->runnable_object) {    # the temp_directory is actually kept in the Process object:
            $runnable_object->cleanup_worker_temp_directory();
        }
    }
}

sub _update_resource_usage {
    my ($worker, $worker_stopwatch, $exception_status) = @_;

    $worker_stopwatch->pause();
    my $resource_usage;
    eval {
        # Try BSD::Resource if present
        my $res_self;
        my $res_child;
        # NOTE: I couldn't find a way of require-ing the module and getting
        # the barewords RUSAGE_* imported
        eval q{
            use BSD::Resource;
            $res_self = BSD::Resource::getrusage(RUSAGE_SELF);
            $res_child = BSD::Resource::getrusage(RUSAGE_CHILDREN);
            };
        return 0 if $@;
        $resource_usage = {
            'exit_status'   => 'done',
            'mem_megs'      => ($res_self->maxrss + $res_child->maxrss) / 1024.,
            'swap_megs'     => undef,
            'pending_sec'   => 0,
            'cpu_sec'       => $res_self->utime + $res_self->stime + $res_child->utime + $res_child->stime,
            'lifespan_sec'  => $worker_stopwatch->get_elapsed(),
            'exception_status' => $exception_status,
        };

    } or eval {
        # Unix::Getrusage otherwise
        require Unix::Getrusage;
        my $res_self = Unix::Getrusage::getrusage();
        my $res_child = Unix::Getrusage::getrusage_children();
        $resource_usage = {
            'exit_status'   => 'done',
            'mem_megs'      => ($res_self->{ru_maxrss} + $res_child->{ru_maxrss}) / 1024.,
            'swap_megs'     => undef,
            'pending_sec'   => 0,
            'cpu_sec'       => $res_self->{ru_utime} + $res_self->{ru_stime} + $res_child->{ru_utime} + $res_child->{ru_stime},
            'lifespan_sec'  => $worker_stopwatch->get_elapsed(),
            'exception_status' => $exception_status,
        };
    };

    # Store the data if one of the above calls was successful
    if ($resource_usage) {
        $worker->adaptor->store_resource_usage(
            {$worker->process_id => $resource_usage},
            {$worker->process_id => $worker->dbID},
        );
    }
}

1;

__DATA__

=pod

=head1 NAME

    runWorker.pl [options]

=head1 DESCRIPTION

    runWorker.pl is an eHive component script that does the work of a single Worker -
    specializes in one of the analyses and starts executing jobs of that analysis one-by-one or batch-by-batch.

    Most of the functionality of the eHive is accessible via beekeeper.pl script,
    but feel free to run the runWorker.pl if you think you know what you are doing :)

=head1 USAGE EXAMPLES

        # Run one local worker process in ehive_dbname and let the system pick up the analysis
    runWorker.pl -url mysql://username:secret@hostname:port/ehive_dbname

        # Run one local worker process in ehive_dbname and let the system pick up the analysis from the given resource_class
    runWorker.pl -url mysql://username:secret@hostname:port/ehive_dbname -rc_name low_mem

        # Run one local worker process in ehive_dbname and constrain its initial specialization within a subset of analyses
    runWorker.pl -url mysql://username:secret@hostname:port/ehive_dbname -analyses_pattern '1..15,analysis_X,21'

        # Run one local worker process in ehive_dbname and allow it to respecialize within a subset of analyses
    runWorker.pl -url mysql://username:secret@hostname:port/ehive_dbname -can_respecialize 1 -analyses_pattern 'blast%-4..6'

        # Run a specific job in a local worker process:
    runWorker.pl -url mysql://username:secret@hostname:port/ehive_dbname -job_id 123456

=head1 OPTIONS

=head2 Connection parameters:

    -reg_conf <path>            : path to a Registry configuration file
    -reg_alias <string>         : species/alias name for the Hive DBAdaptor
    -reg_type <string>          : type of the registry entry ('hive', 'core', 'compara', etc - defaults to 'hive')
    -url <url string>           : url defining where database is located
    -nosqlvc <0|1>              : skip sql version check if 1

=head2 Task specification parameters:

    -rc_id <id>                 : resource class id
    -rc_name <string>           : resource class name
    -analyses_pattern <string>  : restrict the specialization of the Worker to the specified subset of Analyses
    -analysis_id <id>           : run a worker and have it specialize to an analysis with this analysis_id
    -job_id <id>                : run a specific job defined by its database id
    -force 0|1                  : set to 1 if you want to force running a Worker over a BLOCKED analysis or to run a specific DONE/SEMAPHORED job_id

=head2 Worker control parameters:

    -job_limit <num>            : #jobs to run before worker can die naturally
    -life_span <num>            : number of minutes this worker is allowed to run
    -no_cleanup                 : don't perform temp directory cleanup when worker exits
    -no_write                   : don't write_output or auto_dataflow input_job
    -hive_log_dir <path>        : directory where stdout/stderr of the whole hive of workers is redirected
    -worker_log_dir <path>      : directory where stdout/stderr of this particular worker is redirected
    -retry_throwing_jobs <0|1>  : if a job dies *knowingly*, should we retry it by default?
    -can_respecialize <0|1>     : allow this worker to re-specialize into another analysis (within resource_class) after it has exhausted all jobs of the current one

=head2 Other options:

    -help                       : print this help
    -versions                   : report both Hive code version and Hive database schema version
    -debug <level>              : turn on debug messages at <level>

=head1 LICENSE

    Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Copyright [2016-2017] EMBL-European Bioinformatics Institute

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.

=head1 CONTACT

    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates

=cut

