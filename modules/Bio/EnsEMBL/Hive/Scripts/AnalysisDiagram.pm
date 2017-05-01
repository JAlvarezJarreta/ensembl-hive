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


package Bio::EnsEMBL::Hive::Scripts::AnalysisDiagram;

use strict;
use warnings;


use Bio::EnsEMBL::Hive::Utils ('load_file_or_module');
use Bio::EnsEMBL::Hive::Utils::Graph;


sub add_connection_command_line_options { 1 };

sub command_line_options {
    return [

            # json config files
        'config_file=s@'        => 'config_files',

        'pipeconfig|pc=s@'      => 'pipeconfigs', # now an array

        'f|format=s'            => 'format',
        'o|out|output=s'        => 'output',
        'dot_input=s'           => 'dot_input',   # filename to store the intermediate dot input (valuable for debugging)

    ];
}


sub run {
    my $self = shift;

    # FIXME: Allow skipping -url and -reg_alias, In this case initialize 
    #$self->param('pipeline') = Bio::EnsEMBL::Hive::HivePipeline->new();

    foreach my $pipeconfig (@{ $self->param('pipeconfigs') }) {
        my $pipeconfig_package_name = load_file_or_module( $pipeconfig );

        my $pipeconfig_object = $pipeconfig_package_name->new();
        $pipeconfig_object->process_options( 0 );

        $pipeconfig_object->add_objects_from_config( $self->param('pipeline') );
    }

    if($self->param('output')) {

        if(!$self->param('format')) {
            if($self->param('output')=~/\.(\w+)$/) {
                $self->param('format') = $1;
            } else {
                die "Format was not set and could not guess from ".$self->param('output').". Please use either way to select it.\n";
            }
        }

        if($self->param('format') eq 'txt') {
            local *STDOUT;

            open (STDOUT, '>', $self->param('output')); # redirect STDOUT to $self->param('output')

            $self->param('pipeline')->print_diagram;     # and capture the Unicode diagram in a text file

        } else {
            my $graph = Bio::EnsEMBL::Hive::Utils::Graph->new($self->param('pipeline'), @{ $self->{'config_files'} });
            my $graphviz = $graph->build();

            if($self->param('dot_input')) {
                $graphviz->dot_input_filename( $self->param('dot_input') );
            }

            my $call = 'as_'.$self->param('format');

            $graphviz->$call($self->param('output'));
        }

    } else {
        $self->param('pipeline')->print_diagram;

        print "\n";
        print "----------------------------------------------------------\n";
        print "   Did you forget to specify the -output flowchart.png ?  \n";
        print "----------------------------------------------------------\n";
    }
}


1;

__DATA__

=pod

=head1 NAME

    generate_graph.pl

=head1 SYNOPSIS

    ./generate_graph.pl -help

    ./generate_graph.pl [ -url mysql://user:pass@server:port/dbname | -reg_conf <reg_conf_file> -reg_alias <reg_alias> ] [-pipeconfig TopUp_conf.pm]* -output OUTPUT_LOC

=head1 DESCRIPTION

    This program will generate a graphical representation of your hive pipeline.
    This includes visualising the flow of data from the different analyses, blocking
    rules & table writers. The graph is also coloured to indicate the stage
    an analysis is at. The colours & fonts used can be configured via
    hive_config.json configuration file.

=head1 OPTIONS

B<--url>

    url defining where hive database is located

B<--reg_conf>

    path to a Registry configuration file

B<--reg_alias>

    species/alias name for the Hive DBAdaptor

B<--nosqlvc>

    if 1, don't check sql schema version

B<--config_file>

    Path to JSON hive config file

B<--pipeconfig>

    A pipeline configuration file that can function both as the initial source of pipeline structure or as a top-up config.
    This option can now be used multiple times for multiple top-ups.

B<--format>

    (Optional) specify the output format, or override the output format specified by the output file's extension
    (e.g. png, jpeg, dot, gif, ps)

B<--output>

    Location of the file to write to.
    The file extension (.png , .jpeg , .dot , .gif , .ps) will define the output format.

B<--help>

    Print this help message

=head1 EXTERNAL DEPENDENCIES

    GraphViz

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

