# kafka-hot-crm

App for å importere relevante data fra Kafka til Salesforce.
Én app-instans per Kafka-kø.
For eksempel:
* sf-plis

Kafka-eventer kan filtreres og modifiseres programmatisk per app, se [Bootstrap.kt](src/main/kotlin/no/nav/sf/pdl/kafka/Bootstrap.kt).

Spesifikk NAIS-konfigurasjon per app ligger under [.nais](.nais).

# NB! Deploy vid push styrd av main.yaml
Merk at statusen til [main.yaml](.github/workflows/main.yml) bestemmer hvilken app som blir deployet og hvor ved push.

Hvis du bruker main.yml for kjøringer med flagg satt til TRUE (FLAG_SEEK, FLAG_NO_POST, FLAG_ALT_ID), husk å sette dem tilbake til FALSE når du er ferdig.

Alternativt kan du bruke dispatch-jobben for kjøringer med flagg satt til TRUE, slik at de kun er aktive for den spesifikke deployen. Husk at du likevel må dispatche med flagg satt til FALSE når du er ferdig, med tanke på at restarter av image i miljøet kan inntreffe

En beskrivelse av hva flaggene gjør finner du i [main.yaml](.github/workflows/main.yml).

<details>
<summary><strong>Flyt</strong></summary>

[![](https://mermaid.ink/img/pako:eNpdU0tu2zAQvcqAa8uQ_4oWBWI7Tty0aNAEXVTOgpZGMiGKJEg6aer4OL1D97lYh3TaBN2JfN8ZQgdW6gpZzmqpH8sdtx7ulhsFcF5c87rlcKeNKCGBWlvA1mFnUIKrE1PJpA2Me0iSDzAv5lp75y03_dYT_xM6tFBqVYsGdAP4gMrvUUp43AmPUjh_H3LmUb54TVto5fYdCRNABUI5z5UDQxfty-_IX0T-srjRZKWNAS8kDNMUOpSVUA1Ra8sh2kXBMgouipWQ3hJKXailcjRQCHqTVWid7t7qAd3x1ouHaHMRbVbFLaoKttyXOzAvv6DjrfsvPhS65RJDQolwfrOOBqtocHn4is7QlLHlG-0YKJeB8hzcvlw_w1Wx0F0nPOi6duhBnIYKA0jdNGDR7aXnpzWepCsU8hnWxZqKKFp-lCfvuln09gkdPGAFCp1HMLTH6HAV-30sruidXrE4ZgTXJ3CjWI81VlQs93aPPUYb7Hg4skOgbZjfYYcbltNnhTWnghvWewd941bwLU0dOFEToC0v28bqvapO0vhAhluqsmGBROs5UrTh6rvW3d90UjQ7ltdcOjrtTcU9LgVvLO_-3ZIHPeyCvD3LJ9NxNGH5gf1g-Wg07I8H6WQ2G2bpeJSOpz32xPLpoJ8Nsskgm02Gw2w6Ozv22M8Ym_azUTqcptloPD6bEDTrMayE1_bz6S-KP9PxDwjTFrk?type=png)](https://mermaid.live/edit#pako:eNpdU0tu2zAQvcqAa8uQ_4oWBWI7Tty0aNAEXVTOgpZGMiGKJEg6aer4OL1D97lYh3TaBN2JfN8ZQgdW6gpZzmqpH8sdtx7ulhsFcF5c87rlcKeNKCGBWlvA1mFnUIKrE1PJpA2Me0iSDzAv5lp75y03_dYT_xM6tFBqVYsGdAP4gMrvUUp43AmPUjh_H3LmUb54TVto5fYdCRNABUI5z5UDQxfty-_IX0T-srjRZKWNAS8kDNMUOpSVUA1Ra8sh2kXBMgouipWQ3hJKXailcjRQCHqTVWid7t7qAd3x1ouHaHMRbVbFLaoKttyXOzAvv6DjrfsvPhS65RJDQolwfrOOBqtocHn4is7QlLHlG-0YKJeB8hzcvlw_w1Wx0F0nPOi6duhBnIYKA0jdNGDR7aXnpzWepCsU8hnWxZqKKFp-lCfvuln09gkdPGAFCp1HMLTH6HAV-30sruidXrE4ZgTXJ3CjWI81VlQs93aPPUYb7Hg4skOgbZjfYYcbltNnhTWnghvWewd941bwLU0dOFEToC0v28bqvapO0vhAhluqsmGBROs5UrTh6rvW3d90UjQ7ltdcOjrtTcU9LgVvLO_-3ZIHPeyCvD3LJ9NxNGH5gf1g-Wg07I8H6WQ2G2bpeJSOpz32xPLpoJ8Nsskgm02Gw2w6Ozv22M8Ym_azUTqcptloPD6bEDTrMayE1_bz6S-KP9PxDwjTFrk)

</details>
