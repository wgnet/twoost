# coding: utf-8

from __future__ import print_function, division, absolute_import

from twisted.trial.unittest import TestCase

from twoost import conf


class ConfTest(TestCase):

    def test_empty_config(self):
        s = conf.Settings()
        self.assertRaises(AttributeError, lambda: s.SOME_PROP)
        self.assertEqual(123, s.get('SOME_PROP', 123))
        self.assertEqual(set(), s.keys())

    def test_config_stack(self):
        s = conf.Settings()
        s.add_config({"A": 1})
        s.add_config({"B": 3})
        s.add_config({"A": 2})

        self.assertSetEqual({"A", "B"}, s.keys())
        self.assertEqual(2, s.A)
        self.assertEqual(3, s.B)

        self.assertIsNone(s.get("X"))

    def test_conf_stack_mod(self):

        s = conf.Settings()
        x1 = s.add_config({"A": 1})
        x2 = s.add_config({"A": 2})
        x3 = s.add_config({"A": 3})

        self.assertEqual(3, s.A)

        s.remove_config(x2)
        self.assertEqual(3, s.A)

        self.assertRaises(ValueError, s.remove_config, x2)

        s.remove_config(x3)
        self.assertEqual(1, s.A)

        x4 = s.add_config({"C": 123})
        self.assertEqual(1, s.A)

        x5 = s.add_config({"A": 123})
        self.assertEqual(123, s.A)
        self.assertSetEqual({"A", "C"}, s.keys())

        s.remove_config(x5)
        s.remove_config(x1)
        self.assertSetEqual({"C"}, s.keys())

        s.remove_config(x4)
        self.assertSetEqual(set(), s.keys())

    def test_magic_props(self):

        zcalcs = [0]

        def calcz():
            zcalcs[0] += 1
            return 33

        s = conf.Settings()
        s.add_config({
            "A": 3,
            "B": conf.prop_dynamic(lambda x: x.A + 10),
            "Z": conf.prop_lazy(calcz),
        })

        self.assertEqual(33, s.Z)
        self.assertEqual(33, s.Z)
        self.assertEqual(33, s.Z)
        self.assertEqual(1, zcalcs[0])

    def test_dynamic_prop(self):

        s = conf.Settings()
        s.add_config({
            "A": 3,
            "B": conf.prop_dynamic(lambda x: x.A + 10),
        })

        self.assertEqual(13, s.B)
        x1 = s.add_config({"A": 5})
        self.assertEqual(15, s.B)
        s.remove_config(x1)
        self.assertEqual(13, s.B)

    def test_lazy_prop(self):

        zcalcs = [0]

        def calcz():
            zcalcs[0] += 1
            return 33

        s = conf.Settings()
        s.add_config({
            "Z": conf.prop_lazy(calcz),
        })

        self.assertEqual(33, s.Z)
        self.assertEqual(33, s.Z)
        self.assertEqual(33, s.Z)
        self.assertEqual(1, zcalcs[0])

    def test_alter_prop(self):

        s = conf.Settings()
        s.add_config({
            "B": 3,
        })
        s.add_config({
            "B": conf.prop_alter(lambda x: x + 10),
        })

        self.assertEqual(13, s.B)
        s.add_config({
            "B": conf.prop_alter(lambda x: x + 5),
        })
        self.assertEqual(18, s.B)

    def test_merge_prop(self):

        s = conf.Settings()
        s.add_config({
            "A": [1, 2],
            "B": {"x": [1, 2], "y": {"c": 3, "f": 2}, "z": 1},
            "C": {1, 3},
        })
        x1 = s.add_config({
            "A": conf.prop_merge([3, 4]),
            "B": conf.prop_merge({"x": [3], "y": {"c": 5, "p": 3}, "z": 4}),
            "C": conf.prop_merge({5, 3}),
        })

        self.assertEqual([1, 2, 3, 4], s.A)
        self.assertEqual({"x": [1, 2, 3], "y": {"c": 5, "p": 3, "f": 2}, "z": 4}, s.B)
        self.assertSetEqual({1, 3, 5}, s.C)

        s.remove_config(x1)
        self.assertEqual([1, 2], s.A)
        self.assertEqual({"x": [1, 2], "y": {"c": 3, "f": 2}, "z": 1}, s.B)
        self.assertSetEqual({1, 3}, s.C)

    def test_class_conf(self):

        class SomeConf(conf.Config):
            FOO = 1
            BAR = 2

        class AnotherConf(conf.Config):
            FOO = 3
            ZAG = 5

        s = conf.Settings()
        s.add_config(SomeConf())
        s.add_config(AnotherConf())

        self.assertSetEqual({"FOO", "BAR", "ZAG"}, s.keys())
        self.assertEqual(3, s.FOO)
        self.assertEqual(2, s.BAR)
        self.assertEqual(5, s.ZAG)

    def test_dd_merge_pure(self):

        d1 = {1: 2, 3: 4}
        d2 = {3: 5}
        self.assertEqual({1: 2, 3: 5}, conf.dd_merge(d1, d2))
        self.assertEqual({1: 2, 3: 4}, d1)
        self.assertEqual({3: 5}, d2)

        l1 = [1, 2]
        l2 = [3, 4]
        self.assertEqual([1, 2, 3, 4], conf.dd_merge(l1, l2))
        self.assertEqual([1, 2], l1)
        self.assertEqual([3, 4], l2)
