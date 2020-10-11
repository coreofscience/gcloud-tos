import json

def create_tree(event, context):

    print('event', event)
    print('context', context)

    if 'params' in event:
        print('Path parameters:')
        for param, value in event['params'].items():
            print(f'  {param}: {value}')

    print('Function triggered by change to:',  context.resource)
    print('Admin?: %s' % event.get("admin", False))
    print('Delta:')
    print(json.dumps(event["delta"]))
