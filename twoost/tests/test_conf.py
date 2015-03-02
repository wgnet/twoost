# coding: utf-8

from __future__ import print_function, division, absolute_import

from twisted.trial.unittest import TestCase

from twoost import conf


class ConfTest(TestCase):

    def test_empty_config(self):
        s = conf.StackedConfig()
        self.assertRaises(AttributeError, lambda: s.SOME_PROP)
        self.assertEqual(123, s.get('SOME_PROP', 123))
        self.assertEqual(set(), s.keys())

    def test_config_stack(self):
        s = conf.StackedConfig()
        s.add_config({"A": 1})
        s.add_config({"B": 3})
        s.add_config({"A": 2})

        self.assertSetEqual({"A", "B"}, s.keys())
        self.assertEqual(2, s.A)
        self.assertEqual(3, s.B)

        self.assertIsNone(s.get("X"))

    def test_conf_stack_mod(self):

        s = conf.StackedConfig()
        x1 = s.add_config({"A": 1})
        x2 = s.add_config({"A": 2})
        x3 = s.add_config({"A": 3})

        self.assertEqual(3, s.A)

        s.remove_config(x2)
        self.assertEqual(3, s.A)
        s.remove_config(x2)
        self.assertEqual(3, s.A)

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

        s = conf.StackedConfig()
        s.add_config({
            "A": 3,
            "B": conf.dynamic_prop(lambda x: x.A + 10),
            "Z": conf.lazy_prop(calcz),
        })

        self.assertEqual(33, s.Z)
        self.assertEqual(33, s.Z)
        self.assertEqual(33, s.Z)
        self.assertEqual(1, zcalcs[0])

    def test_dynamic_prop(self):

        s = conf.StackedConfig()
        s.add_config({
            "A": 3,
            "B": conf.dynamic_prop(lambda x: x.A + 10),
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

        s = conf.StackedConfig()
        s.add_config({
            "Z": conf.lazy_prop(calcz),
        })

        self.assertEqual(33, s.Z)
        self.assertEqual(33, s.Z)
        self.assertEqual(33, s.Z)
        self.assertEqual(1, zcalcs[0])

    def test_alter_prop(self):

        s = conf.StackedConfig()
        s.add_config({
            "B": 3,
        })
        s.add_config({
            "B": conf.alter_prop(lambda x: x + 10),
        })

        self.assertEqual(13, s.B)
        s.add_config({
            "B": conf.alter_prop(lambda x: x + 5),
        })
        self.assertEqual(18, s.B)

    def test_class_conf(self):

        class SomeConf(conf.Config):
            FOO = 1
            BAR = 2

        class AnotherConf(conf.Config):
            FOO = 3
            ZAG = 5

        s = conf.StackedConfig()
        s.add_config(SomeConf())
        s.add_config(AnotherConf())

        self.assertSetEqual({"FOO", "BAR", "ZAG"}, s.keys())
        self.assertEqual(3, s.FOO)
        self.assertEqual(2, s.BAR)
        self.assertEqual(5, s.ZAG)
