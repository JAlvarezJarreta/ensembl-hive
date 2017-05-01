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

my %legacy_scripts = (
    'seed_pipeline.pl'  => 'Seed',
    'tweak_pipeline.pl' => 'Tweak',
    'init_pipeline.pl'  => 'InitPipeline',
    'db_cmd.pl'         => 'DbCmd',
    'runWorker.pl'      => 'RunWorker',
    'hoover_pipeline.pl'    => 'Hoover',
    'generate_timeline.pl'  => 'Timeline',
    'generate_graph.pl'     => 'AnalysisDiagram',
    'visualize_jobs.pl'     => 'JobDiagram',
    'load_resource_usage.pl'    => 'LoadResourceUsage',
);

my %recognized_actions = (
    # Aliases
    'version'   => 'Versions',
    'init'      => 'InitPipeline',
    'worker'    => 'RunWorker',
);

foreach my $s (@{ find_submodules('Bio::EnsEMBL::Hive::Scripts') }) {
    next if $s eq 'Bio::EnsEMBL::Hive::Scripts::BaseScript';
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
        die "Syntax error. Use $0 <action> <parameters> where <action is one of: ".join(", ", keys %recognized_actions)."\n";
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

hive.pl <action> [options]

=head1 DESCRIPTION

hive.pl is a generic wrapper for the script module interface. It allows to run all the
eHive commands as "hive init", "hive seed" etc.

=head1 USAGE EXAMPLES


=head1 OPTIONS

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

