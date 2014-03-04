{alias, repdis, "./repdis/"}.
{include, ["../include"]}.
{cover, "./repdis.coverspec"}.
{suites, repdis, [
                  repdis_pd_SUITE
                 ]}.
