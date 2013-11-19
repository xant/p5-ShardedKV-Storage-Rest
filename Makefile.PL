use 5.008005;
use ExtUtils::MakeMaker;
WriteMakefile(
    NAME           => 'ShardedKV::Storage::Rest',
    VERSION_FROM   => 'lib/ShardedKV/Storage/Rest.pm',
    PREREQ_PM      => {
      'Test::More'        => 0,
      'ShardedKV'         => 0,
    },
    ABSTRACT_FROM  => 'lib/ShardedKV/Storage/Rest.pm',
    AUTHOR         => [
        'Andrea Guzzo <xant@cpan.org>',
    ],
);

