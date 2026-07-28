"""Kasal AS an A2A agent — the inbound half.

Everything here ANSWERS someone else's request: the Agent Card, task operations,
streaming, push notifications, and the translation between Kasal's canonical
external shapes and A2A's wire types.

Split from ``a2a_client`` because the two directions have opposite trust models.
Inbound, the caller is untrusted and Kasal decides what to expose; outbound,
Kasal is the one making requests to an address a tenant supplied. Reading a file
should tell you immediately which side of that line you are on.
"""
