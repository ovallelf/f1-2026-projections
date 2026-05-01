#Author-GitHub Copilot
#Description-Change the second largest diameter parameter to a specified value

import adsk.core, adsk.fusion, traceback

def run(context):
    ui = None
    try:
        app = adsk.core.Application.get()
        ui = app.userInterface
        design = adsk.fusion.Design.cast(app.activeProduct)
        
        if not design:
            ui.messageBox('No active Fusion 360 design found.')
            return
        
        # Target diameter value in mm
        new_diameter_mm = 28.3
        new_diameter_cm = new_diameter_mm / 10  # Fusion uses cm internally
        
        # Get all parameters from the design
        all_params = design.allParameters
        
        # Collect all diameter-like parameters with their values
        diameter_params = []
        
        for i in range(all_params.count):
            param = all_params.item(i)
            # Look for parameters that might be diameters (common naming patterns)
            name_lower = param.name.lower()
            if any(keyword in name_lower for keyword in ['diameter', 'dia', 'd_', '_d', 'radius', 'rad']):
                diameter_params.append({
                    'name': param.name,
                    'value': param.value,  # value in cm
                    'expression': param.expression,
                    'param': param
                })
        
        # If no diameter-named params found, look at all numeric parameters
        if len(diameter_params) < 2:
            diameter_params = []
            for i in range(all_params.count):
                param = all_params.item(i)
                try:
                    # Only include parameters with positive values (likely dimensions)
                    if param.value > 0:
                        diameter_params.append({
                            'name': param.name,
                            'value': param.value,
                            'expression': param.expression,
                            'param': param
                        })
                except:
                    pass
        
        if len(diameter_params) < 2:
            ui.messageBox('Could not find at least 2 diameter parameters in the design.')
            return
        
        # Sort by value descending to find second largest
        diameter_params.sort(key=lambda x: x['value'], reverse=True)
        
        # Get the second largest
        second_largest = diameter_params[1]
        
        # Show confirmation dialog
        result = ui.messageBox(
            f"Found second largest parameter:\n\n"
            f"Name: {second_largest['name']}\n"
            f"Current value: {second_largest['value'] * 10:.2f} mm\n\n"
            f"Change to {new_diameter_mm} mm?",
            "Confirm Parameter Change",
            adsk.core.MessageBoxButtonTypes.YesNoButtonType
        )
        
        if result == adsk.core.DialogResults.DialogYes:
            # Change the parameter value
            second_largest['param'].value = new_diameter_cm
            
            ui.messageBox(
                f"Successfully changed '{second_largest['name']}' to {new_diameter_mm} mm",
                "Success"
            )
        else:
            ui.messageBox("Operation cancelled.")
            
    except:
        if ui:
            ui.messageBox('Failed:\n{}'.format(traceback.format_exc()))


def stop(context):
    pass
