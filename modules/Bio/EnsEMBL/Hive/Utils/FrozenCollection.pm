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

=pod

=head1 NAME

Bio::EnsEMBL::Hive::Utils::FrozenCollection - A collection object

=cut

package Bio::EnsEMBL::Hive::Utils::FrozenCollection;

use strict;
use warnings;

use Bio::EnsEMBL::Hive::Utils ('throw');

use base ('Bio::EnsEMBL::Hive::Utils::Collection');

# Override present() ?


sub add {
    my $self = shift @_;
    throw("Cannot add an element to a frozen collection");
}


sub forget {
    my $self = shift @_;
    throw("Cannot remove an element from a frozen collection");
}

sub build_lookup_if_needed {
    my $self    = shift @_;
    my $field   = shift @_;
    return if $self->{'_lookups'}->{$field};
    $self->{'_lookups'}->{$field} = {};
    $self->{'_lookup_undefs'}->{$field} = [];
    $self->_add_element_to_lookup($_, $field) for $self->list;
}
sub _add_element_to_lookup {
    my $self    = shift @_;
    my $element = shift @_;
    my $field   = shift @_;
    my $value   = (ref($element) eq 'HASH') ? $element->{$field} : $element->$field();
    if (defined $value) {
        push @{$self->{'_lookups'}->{$field}->{$value}}, $element;
    } else {
        push @{$self->{'_lookup_undefs'}->{$field}}, $element;
    }
}


sub find_one_by {
    my ($self, %method_to_filter_value) = @_;

    if (scalar(keys %method_to_filter_value) == 1) {
        my ($filter_name) = keys %method_to_filter_value;
        my $filter_value = $method_to_filter_value{$filter_name};
        if (defined $filter_value) {
            if (ref($filter_value) ne 'CODE') {
                return $self->{'_lookups'}->{$filter_name}->{$filter_value}->[0];
            }
        } else {
            $self->build_lookup_if_needed($filter_name);
            return $self->{'_lookup_undefs'}->{$filter_name}->[0];
        }
    }

    return $self->SUPER::find_one_by(%method_to_filter_value);
}


sub find_all_by {
    my ($self, %method_to_filter_value) = @_;

    if (scalar(keys %method_to_filter_value) == 1) {
        my ($filter_name) = keys %method_to_filter_value;
        my $filter_value = $method_to_filter_value{$filter_name};
        if (defined $filter_value) {
            if (ref($filter_value) ne 'CODE') {
                return $self->{'_lookups'}->{$filter_name}->{$filter_value};
            }
        } else {
            $self->build_lookup_if_needed($filter_name);
            return $self->{'_lookup_undefs'}->{$filter_name};
        }
    }

    return $self->SUPER::find_all_by(%method_to_filter_value);
}


1;
