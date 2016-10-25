from botoflow import activities, activity
from botoflow.data_converter.json_data_converter import JSONDataConverter


def test_activities_decorator_adds_data_converter():
    class MyDataConverter(object):
        pass

    @activities(data_converter=MyDataConverter())
    class ActivitiesWithCustomDataConverter(object):
        @activity(None)
        def foobar(self):
            pass

    assert isinstance(ActivitiesWithCustomDataConverter().foobar.swf_options['activity_type'].data_converter, MyDataConverter)


def test_activities_decorator_uses_default_data_converter():
    @activities()
    class ActivitiesWithDefaultDataConverter(object):
        @activity(None)
        def foobar(self):
            pass

    assert isinstance(ActivitiesWithDefaultDataConverter().foobar.swf_options['activity_type'].data_converter, JSONDataConverter)
