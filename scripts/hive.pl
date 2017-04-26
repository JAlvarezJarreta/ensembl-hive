#!/usr/bin/env perl

use strict;
use warnings;

    # Finding out own path in order to reference own components (including own modules):
use Cwd            ();
use File::Basename ();
BEGIN {
    $ENV{'EHIVE_ROOT_DIR'} ||= File::Basename::dirname( File::Basename::dirname( Cwd::realpath($0) ) );
    unshift @INC, $ENV{'EHIVE_ROOT_DIR'}.'/modules';
}

use Data::Dumper;

use Bio::EnsEMBL::Hive::Utils ('find_submodules');

#use Getopt::Long qw(:config no_auto_abbrev);

my %blacklist = map {$_=>1} qw(Bio::EnsEMBL::Hive::Scripts::BaseScript Bio::EnsEMBL::Hive::Scripts::RunWorker Bio::EnsEMBL::Hive::Scripts::StandaloneJob);

my %legacy_scripts = (
    'seed_pipeline.pl'  => 'Seed',
    'tweak_pipeline.pl' => 'Tweak',
    'init_pipeline.pl'  => 'InitPipeline',
);

my %recognized_actions = (
    # Aliases
    'version'   => 'Versions',
    'init'      => 'InitPipeline',
);

foreach my $s (@{ find_submodules('Bio::EnsEMBL::Hive::Scripts') }) {
    next if $blacklist{$s};
    $s =~ s/Bio::EnsEMBL::Hive::Scripts:://;
    $recognized_actions{lc $s} = $s;
}

foreach my $s (keys %legacy_scripts) {
    my $a = $legacy_scripts{$s};
    $s =~ s/\.pl$//;
    # Topup the hash with the script name
    $recognized_actions{lc $s} = $a;
}

#print Dumper(\%recognized_actions);

## Let's try to find the name of the action
my $action;

my $this_script_name = File::Basename::basename(lc $0);
if ($this_script_name =~ /^hive(\.pl)?/) {
    unless (@ARGV) {
        die "Syntax error. Use $0 <action> <parameters>";
    }
    $action = shift @ARGV;
    $action = $recognized_actions{lc $action} || die "Unrecognized action '$action'. Use one of: ".join(", ", keys %recognized_actions)."\n";
} else {
    $action = $legacy_scripts{$this_script_name} or die "Unrecognized script name '$this_script_name'";
}

my $module_name = 'Bio::EnsEMBL::Hive::Scripts::'.(ucfirst $action);

eval "require $module_name";
if ($@) {
    die "Cannot load the module $module_name for action '$action': $@";
} else {
    my $script_object = $module_name->new();
    $script_object->main();
}


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

