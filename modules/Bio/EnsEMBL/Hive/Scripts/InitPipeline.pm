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


package Bio::EnsEMBL::Hive::Scripts::InitPipeline;

use strict;
use warnings;

use Bio::EnsEMBL::Hive::HivePipeline;
use Bio::EnsEMBL::Hive::Utils ('load_file_or_module');

use base ('Bio::EnsEMBL::Hive::Scripts::BaseScript');


sub must_use_all_command_line_options { 0 };

sub command_line_options {
    my @tweaks;
    return [
        'analysis_topup!'       => '_analysis_topup',   # deprecated. now always on
        'job_topup!'            => '_job_topup',        # deprecated. use seed_pipeline.pl instead
        'tweak|SET=s@'          => ['tweaks', \@tweaks],
        'DELETE=s'              => sub { my ($opt_name, $opt_value) = @_; push @tweaks, $opt_value.'#'; },
        'SHOW=s'                => sub { my ($opt_name, $opt_value) = @_; push @tweaks, $opt_value.'?'; },
    ];
}


sub run {
    my $self = shift;

    if($self->param_is_defined('_job_topup')) {
        die "-job_topup mode has been discontinued. Please use seed_pipeline.pl instead.\n";
    }
    if($self->param_is_defined('_analysis_topup')) {
        die "-analysis_topup has been deprecated. Please note this script now *always* runs in -analysis_topup mode.\n";
    }

    my $file_or_module = shift @ARGV or $self->script_usage(1);

    init_pipeline($file_or_module, $self->param('tweaks'));
}


sub init_pipeline {
    my ($file_or_module, $tweaks) = @_;

    my $pipeconfig_package_name = load_file_or_module( $file_or_module );

    my $pipeconfig_object = $pipeconfig_package_name->new();
    die "PipeConfig $pipeconfig_package_name not created\n" unless $pipeconfig_object;
    die "PipeConfig $pipeconfig_package_name is not a Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf\n" unless $pipeconfig_object->isa('Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf');

    $pipeconfig_object->process_options( 1 );

    $pipeconfig_object->run_pipeline_create_commands();

    my $pipeline = Bio::EnsEMBL::Hive::HivePipeline->new(
                -url => $pipeconfig_object->pipeline_url(),
                -no_sql_schema_version_check => !$pipeconfig_object->is_analysis_topup );

    my $hive_dba = $pipeline->hive_dba()
        or die "HivePipeline could not be created for ".$pipeconfig_object->pipeline_url();

    $pipeconfig_object->add_objects_from_config( $pipeline );

    if($tweaks and @$tweaks) {
        $pipeline->apply_tweaks( $tweaks );
    }

    $pipeline->save_collections();

    print $pipeconfig_object->useful_commands_legend();

    $hive_dba->dbc->disconnect_if_idle;     # This is necessary because the current lack of global DBC caching may leave this DBC connected and prevent deletion of the DB in pgsql mode

    return $hive_dba->dbc->url;
}


1;

__DATA__

=pod

=head1 NAME

    init_pipeline.pl

=head1 SYNOPSIS

    init_pipeline.pl <config_module_or_filename> [<options_for_this_particular_pipeline>]

=head1 DESCRIPTION

    init_pipeline.pl is a generic script that is used to create+setup=initialize eHive pipelines from PipeConfig configuration modules.

=head1 USAGE EXAMPLES

        # get this help message:
    init_pipeline.pl

        # initialize a generic eHive pipeline:
    init_pipeline.pl Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf -password <yourpassword>

        # initialize the long multiplicaton pipeline by supplying not only mandatory but also optional data:
        #   (assuming your current directory is ensembl-hive/modules/Bio/EnsEMBL/Hive/PipeConfig) :
    init_pipeline.pl LongMult_conf -password <yourpassword> -first_mult 375857335 -second_mult 1111333355556666 

=head1 OPTIONS

    -hive_force_init <0|1> :  If set to 1, forces the (re)creation of the hive database even if a previous version of it is present in the server.
    -tweak <string>        :  Apply tweaks to the pipeline. See tweak_pipeline.pl for details of tweaking syntax
    -DELETE                :  Delete pipeline parameter (shortcut for tweak DELETE)
    -SHOW                  :  Show  pipeline parameter  (shortcut for tweak SHOW)
    -h | --help            :  Show this help message

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

